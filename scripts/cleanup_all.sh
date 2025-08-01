#!/bin/bash

# Comprehensive Cleanup Script for Banking ES
# This script cleans up both database and Kafka in the correct order

set -e

# Configuration
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-banking_es}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-Francisco1}"
KAFKA_BIN="${KAFKA_BIN:-/home/kafka/kafka/bin}"
BROKER="${BROKER:-localhost:9092}"

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

log_header() {
    echo -e "${CYAN}================================${NC}"
    echo -e "${CYAN} $1${NC}"
    echo -e "${CYAN}================================${NC}"
}

# Function to check prerequisites
check_prerequisites() {
    log_step "Checking Prerequisites"
    
    # Check if psql is available
    if ! command -v psql &> /dev/null; then
        log_error "psql is not installed or not in PATH"
        exit 1
    fi
    
    # Check if Kafka tools are available
    if [ ! -f "$KAFKA_BIN/kafka-topics.sh" ]; then
        log_error "Kafka tools not found at $KAFKA_BIN"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Function to check database connectivity
check_database() {
    log_info "Checking database connectivity..."
    if ! PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1;" >/dev/null 2>&1; then
        log_error "Cannot connect to database at $DB_HOST:$DB_PORT"
        log_error "Please ensure PostgreSQL is running and accessible"
        exit 1
    fi
    log_success "Database is accessible"
}

# Function to check Kafka connectivity
check_kafka() {
    log_info "Checking Kafka connectivity..."
    if ! "$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BROKER" >/dev/null 2>&1; then
        log_error "Cannot connect to Kafka at $BROKER"
        log_error "Please ensure Kafka is running and accessible"
        exit 1
    fi
    log_success "Kafka is accessible"
}

# Function to stop CDC services
stop_cdc_services() {
    log_step "Stopping CDC Services"
    
    # Stop any running CDC consumers
    log_info "Stopping CDC consumers..."
    
    # Kill any processes that might be consuming from CDC topics
    local cdc_processes
    cdc_processes=$(pgrep -f "banking-es.*cdc" || true)
    
    if [ -n "$cdc_processes" ]; then
        log_info "Found CDC processes: $cdc_processes"
        echo "$cdc_processes" | xargs kill -TERM 2>/dev/null || true
        sleep 2
        echo "$cdc_processes" | xargs kill -KILL 2>/dev/null || true
        log_success "Stopped CDC processes"
    else
        log_info "No CDC processes found"
    fi
}

# Function to cleanup Kafka topics
cleanup_kafka_topics() {
    log_step "Cleaning up Kafka Topics"
    
    local topics=(
        "banking-es.public.kafka_outbox_cdc"
        "banking-es-events"
        "banking-es-cache"
        "banking-es-dlq"
        "banking-es-cache-invalidation"
        "schema-changes.banking-es"
    )
    
    for topic in "${topics[@]}"; do
        log_info "Cleaning topic: $topic"
        
        # Get partitions for the topic
        local partitions
        partitions=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKER" --topic "$topic" --describe 2>/dev/null | grep -Eo 'Partition: [0-9]+' | awk '{print $2}' || echo "")
        
        if [ -n "$partitions" ]; then
            # Create temporary JSON file for delete records
            local tmp_json
            tmp_json=$(mktemp)
            
            echo '{ "partitions": [' > "$tmp_json"
            local first=1
            
            for partition in $partitions; do
                if [ $first -eq 0 ]; then
                    echo ',' >> "$tmp_json"
                fi
                echo -n "{\"topic\": \"$topic\", \"partition\": $partition, \"offset\": -1}" >> "$tmp_json"
                first=0
            done
            
            echo '], "version":1 }' >> "$tmp_json"
            
            # Execute delete records
            if "$KAFKA_BIN/kafka-delete-records.sh" --bootstrap-server "$BROKER" --offset-json-file "$tmp_json" >/dev/null 2>&1; then
                log_success "Cleaned topic: $topic"
            else
                log_warning "Failed to clean topic: $topic"
            fi
            
            # Clean up temporary file
            rm -f "$tmp_json"
        else
            log_info "Topic $topic does not exist or has no partitions"
        fi
    done
}

# Function to reset consumer groups
reset_consumer_groups() {
    log_step "Resetting Consumer Groups"
    
    local consumer_groups=(
        "banking-es-group"
        "banking-es-dlq-recovery"
    )
    
    for group in "${consumer_groups[@]}"; do
        log_info "Resetting consumer group: $group"
        
        # Get topics for the consumer group
        local topics
        topics=$("$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BROKER" --group "$group" --describe 2>/dev/null | grep -v "TOPIC" | awk '{print $1}' | sort -u || echo "")
        
        if [ -n "$topics" ]; then
            # Reset offsets for each topic
            for topic in $topics; do
                log_info "Resetting offsets for topic: $topic"
                "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BROKER" --group "$group" --topic "$topic" --reset-offsets --to-earliest --execute >/dev/null 2>&1 || {
                    log_warning "Failed to reset offsets for topic $topic"
                }
            done
            log_success "Reset consumer group: $group"
        else
            log_info "No topics found for consumer group $group"
        fi
    done
}

# Function to create missing partitions
create_missing_partitions() {
    log_step "Creating Missing Partitions"
    
    # Function to run SQL command
    run_sql() {
        local sql="$1"
        local description="$2"
        
        log_info "Running: $description"
        PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$sql" >/dev/null 2>&1 || {
            log_warning "Failed to execute: $description"
        }
    }
    
    # Get current year and month
    local current_year=$(date +%Y)
    local current_month=$(date +%m)
    local next_month=$(date -d "$current_year-$current_month-01 +1 month" +%Y-%m)
    
    log_info "Creating partitions for current month: $current_year-$current_month"
    log_info "Creating partitions for next month: $next_month"
    
    # Create current month partition
    local current_partition="transaction_projections_${current_year}_${current_month}"
    local current_start="${current_year}-${current_month}-01 03:00:00+03"
    local current_end="${next_month}-01 03:00:00+03"
    
    run_sql "CREATE TABLE IF NOT EXISTS $current_partition PARTITION OF transaction_projections FOR VALUES FROM ('$current_start') TO ('$current_end');" "Creating $current_partition partition"
    
    # Create next month partition
    local next_year=$(echo $next_month | cut -d'-' -f1)
    local next_month_num=$(echo $next_month | cut -d'-' -f2)
    local next_partition="transaction_projections_${next_year}_${next_month_num}"
    local next_start="${next_month}-01 03:00:00+03"
    local next_end=$(date -d "$next_month-01 +2 months" +%Y-%m-01)
    local next_end="${next_end} 03:00:00+03"
    
    run_sql "CREATE TABLE IF NOT EXISTS $next_partition PARTITION OF transaction_projections FOR VALUES FROM ('$next_start') TO ('$next_end');" "Creating $next_partition partition"
    
    # Create a few more months ahead for safety
    for i in {2..4}; do
        local future_month=$(date -d "$next_month-01 +$i months" +%Y-%m)
        local future_year=$(echo $future_month | cut -d'-' -f1)
        local future_month_num=$(echo $future_month | cut -d'-' -f2)
        local future_partition="transaction_projections_${future_year}_${future_month_num}"
        local future_start="${future_month}-01 03:00:00+03"
        local future_end=$(date -d "$future_month-01 +1 month" +%Y-%m-01)
        local future_end="${future_end} 03:00:00+03"
        
        run_sql "CREATE TABLE IF NOT EXISTS $future_partition PARTITION OF transaction_projections FOR VALUES FROM ('$future_start') TO ('$future_end');" "Creating $future_partition partition"
    done
    
    log_success "Partition creation completed"
}

# Function to cleanup database
cleanup_database() {
    log_step "Cleaning up Database"
    
    # Function to run SQL command
    run_sql() {
        local sql="$1"
        local description="$2"
        
        log_info "Running: $description"
        PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$sql" >/dev/null 2>&1 || {
            log_warning "Failed to execute: $description"
        }
    }
    
    # Function to check if table exists
    table_exists() {
        local table_name="$1"
        local result
        result=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '$table_name');" 2>/dev/null || echo "f")
        echo "$result" | tr -d ' '
    }
    
    # Disable triggers temporarily
    log_info "Disabling triggers..."
    run_sql "SET session_replication_role = replica;" "Disabling triggers"
    
    # Clean up tables
    local tables=(
        "kafka_outbox_cdc"
        "kafka_outbox"
        "events"
        "accounts"
        "transactions"
        "users"
        "account_projections"
        "transaction_projections"
        "schema_changes"
    )
    
    for table in "${tables[@]}"; do
        if [ "$(table_exists "$table")" = "t" ]; then
            run_sql "TRUNCATE TABLE $table RESTART IDENTITY CASCADE;" "Cleaning $table table"
            log_success "Cleaned $table table"
        else
            log_info "Table $table does not exist"
        fi
    done
    
    # Clean up functions and views
    run_sql "DROP FUNCTION IF EXISTS migrate_outbox_to_cdc() CASCADE;" "Dropping migration function"
    run_sql "DROP FUNCTION IF EXISTS get_outbox_migration_stats() CASCADE;" "Dropping stats function"
    run_sql "DROP FUNCTION IF EXISTS cleanup_old_outbox_after_migration() CASCADE;" "Dropping cleanup function"
    run_sql "DROP VIEW IF EXISTS outbox_migration_status CASCADE;" "Dropping migration status view"
    run_sql "DROP VIEW IF EXISTS cdc_status CASCADE;" "Dropping CDC status view"
    run_sql "DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;" "Dropping update function"
    
    # Reset sequences
    log_info "Resetting sequences..."
    local sequences=(
        "events_id_seq"
        "accounts_id_seq"
        "transactions_id_seq"
        "users_id_seq"
        "kafka_outbox_id_seq"
        "kafka_outbox_cdc_id_seq"
    )
    
    for seq in "${sequences[@]}"; do
        run_sql "SELECT setval('$seq', 1, false);" "Resetting sequence $seq" 2>/dev/null || true
    done
    
    # Clean up Debezium replication slots
    log_info "Cleaning up Debezium replication slots..."
    local slots
    slots=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '%debezium%' OR slot_name LIKE '%banking%';" 2>/dev/null || echo "")
    
    if [ -n "$slots" ]; then
        echo "$slots" | while read -r slot; do
            slot=$(echo "$slot" | tr -d ' ')
            if [ -n "$slot" ]; then
                log_info "Dropping replication slot: $slot"
                run_sql "SELECT pg_drop_replication_slot('$slot');" "Dropping replication slot $slot" 2>/dev/null || true
            fi
        done
    fi
    
    # Clean up publications
    log_info "Cleaning up publications..."
    local publications
    publications=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT pubname FROM pg_publication WHERE pubname LIKE '%debezium%' OR pubname LIKE '%banking%';" 2>/dev/null || echo "")
    
    if [ -n "$publications" ]; then
        echo "$publications" | while read -r pub; do
            pub=$(echo "$pub" | tr -d ' ')
            if [ -n "$pub" ]; then
                log_info "Dropping publication: $pub"
                run_sql "DROP PUBLICATION IF EXISTS $pub;" "Dropping publication $pub" 2>/dev/null || true
            fi
        done
    fi
    
    # Re-enable triggers
    log_info "Re-enabling triggers..."
    run_sql "SET session_replication_role = DEFAULT;" "Re-enabling triggers"
    
    log_success "Database cleanup completed"
}

# Function to restart Debezium connector
restart_debezium_connector() {
    log_step "Restarting Debezium Connector"
    
    # Check if Kafka Connect is accessible
    if curl -s http://localhost:8083/connectors >/dev/null 2>&1; then
        log_info "Restarting Debezium connector..."
        if curl -X POST http://localhost:8083/connectors/banking-es-connector/restart >/dev/null 2>&1; then
            log_success "Debezium connector restarted"
        else
            log_warning "Failed to restart Debezium connector"
        fi
    else
        log_warning "Kafka Connect not accessible at localhost:8083"
    fi
}

# Function to show final status
show_final_status() {
    log_step "Final Status"
    
    # Show database status
    log_info "Database tables:"
    local tables=(
        "kafka_outbox_cdc"
        "kafka_outbox"
        "events"
        "accounts"
        "account_projections"
    )
    
    for table in "${tables[@]}"; do
        local count
        count=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM $table;" 2>/dev/null | tr -d ' ' || echo "N/A")
        log_info "  $table: $count rows"
    done
    
    # Show Kafka topics status
    log_info "Kafka topics:"
    local topics=(
        "banking-es.public.kafka_outbox_cdc"
        "banking-es-events"
        "banking-es-dlq"
    )
    
    for topic in "${topics[@]}"; do
        local partitions
        partitions=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKER" --topic "$topic" --describe 2>/dev/null | grep -Eo 'Partition: [0-9]+' | wc -l || echo "0")
        log_info "  $topic: $partitions partitions"
    done
}

# Main cleanup function
cleanup_all() {
    log_header "Comprehensive Banking ES Cleanup"
    
    # Check prerequisites
    check_prerequisites
    
    # Check connectivity
    check_database
    check_kafka
    
    # Show initial status
    log_step "Initial Status"
    show_final_status
    
    # Confirm cleanup
    log_warning "This will DELETE ALL DATA from both database and Kafka!"
    read -p "Are you absolutely sure you want to continue? Type 'YES' to confirm: " -r
    if [ "$REPLY" != "YES" ]; then
        log_info "Cleanup cancelled by user."
        exit 0
    fi
    
    # Stop CDC services first
    stop_cdc_services
    
    # Cleanup in the correct order
    cleanup_kafka_topics
    reset_consumer_groups
    cleanup_database
    
    # Create missing partitions after cleanup
    create_missing_partitions
    
    # Restart Debezium connector
    restart_debezium_connector
    
    # Show final status
    show_final_status
    
    log_success "Comprehensive cleanup completed successfully!"
}

# Function to show help
show_help() {
    echo "Comprehensive Banking ES Cleanup Script"
    echo ""
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  cleanup    - Clean up all data (database and Kafka)"
    echo "  status     - Show current status"
    echo "  help       - Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  DB_HOST     - Database host (default: localhost)"
    echo "  DB_PORT     - Database port (default: 5432)"
    echo "  DB_NAME     - Database name (default: banking_es)"
    echo "  DB_USER     - Database user (default: postgres)"
    echo "  DB_PASSWORD - Database password (default: Francisco1)"
    echo "  KAFKA_BIN   - Kafka bin directory (default: /home/kafka/kafka/bin)"
    echo "  BROKER      - Kafka broker address (default: localhost:9092)"
    echo ""
}

# Main script logic
case "${1:-cleanup}" in
    "cleanup")
        cleanup_all
        ;;
    "status")
        check_prerequisites
        check_database
        check_kafka
        show_final_status
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