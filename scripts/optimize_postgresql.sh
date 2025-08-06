#!/bin/bash

# PostgreSQL Optimization Script for High Concurrency Banking Application
# This script helps adjust PostgreSQL settings for better performance

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔧 PostgreSQL Optimization Script for High Concurrency${NC}"
echo "=================================================="

# Configuration variables
DB_NAME="banking_es"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"

# Get system information
TOTAL_RAM=$(free -m | awk 'NR==2{printf "%.0f", $2}')
AVAILABLE_RAM=$(free -m | awk 'NR==2{printf "%.0f", $7}')
CPU_CORES=$(nproc)

echo -e "${YELLOW}📊 System Information:${NC}"
echo "  Total RAM: ${TOTAL_RAM}MB"
echo "  Available RAM: ${AVAILABLE_RAM}MB"
echo "  CPU Cores: ${CPU_CORES}"
echo ""

# Calculate recommended settings
RECOMMENDED_MAX_CONNECTIONS=$((CPU_CORES * 4 + 968))
RECOMMENDED_SHARED_BUFFERS=$((TOTAL_RAM / 4))
RECOMMENDED_EFFECTIVE_CACHE_SIZE=$((TOTAL_RAM * 3 / 4))
RECOMMENDED_WORK_MEM=$((AVAILABLE_RAM / (CPU_CORES * 2)))
RECOMMENDED_MAINTENANCE_WORK_MEM=$((AVAILABLE_RAM / 8))

# Ensure minimum values
RECOMMENDED_SHARED_BUFFERS=$((RECOMMENDED_SHARED_BUFFERS > 256 ? RECOMMENDED_SHARED_BUFFERS : 256))
RECOMMENDED_WORK_MEM=$((RECOMMENDED_WORK_MEM > 4 ? RECOMMENDED_WORK_MEM : 4))
RECOMMENDED_MAINTENANCE_WORK_MEM=$((RECOMMENDED_MAINTENANCE_WORK_MEM > 64 ? RECOMMENDED_MAINTENANCE_WORK_MEM : 64))

echo -e "${YELLOW}📈 Recommended PostgreSQL Settings:${NC}"
echo "  max_connections = ${RECOMMENDED_MAX_CONNECTIONS}"
echo "  shared_buffers = ${RECOMMENDED_SHARED_BUFFERS}MB"
echo "  effective_cache_size = ${RECOMMENDED_EFFECTIVE_CACHE_SIZE}MB"
echo "  work_mem = ${RECOMMENDED_WORK_MEM}MB"
echo "  maintenance_work_mem = ${RECOMMENDED_MAINTENANCE_WORK_MEM}MB"
echo ""

# Function to check if PostgreSQL is running
check_postgresql() {
    if ! pg_isready -h $DB_HOST -p $DB_PORT -U $DB_USER > /dev/null 2>&1; then
        echo -e "${RED}❌ PostgreSQL is not running or not accessible${NC}"
        echo "Please ensure PostgreSQL is running and accessible"
        exit 1
    fi
    echo -e "${GREEN}✅ PostgreSQL is running${NC}"
}

# Function to get current PostgreSQL settings
get_current_settings() {
    echo -e "${YELLOW}📋 Current PostgreSQL Settings:${NC}"

    # Get current settings
    CURRENT_MAX_CONNECTIONS=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SHOW max_connections;" 2>/dev/null | xargs)
    CURRENT_SHARED_BUFFERS=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SHOW shared_buffers;" 2>/dev/null | xargs)
    CURRENT_EFFECTIVE_CACHE_SIZE=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SHOW effective_cache_size;" 2>/dev/null | xargs)
    CURRENT_WORK_MEM=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SHOW work_mem;" 2>/dev/null | xargs)
    CURRENT_MAINTENANCE_WORK_MEM=$(psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SHOW maintenance_work_mem;" 2>/dev/null | xargs)
    
    echo "  max_connections = ${CURRENT_MAX_CONNECTIONS:-'Not set'}"
    echo "  shared_buffers = ${CURRENT_SHARED_BUFFERS:-'Not set'}"
    echo "  effective_cache_size = ${CURRENT_EFFECTIVE_CACHE_SIZE:-'Not set'}"
    echo "  work_mem = ${CURRENT_WORK_MEM:-'Not set'}"
    echo "  maintenance_work_mem = ${CURRENT_MAINTENANCE_WORK_MEM:-'Not set'}"
    echo ""
}

# Function to create optimized postgresql.conf
create_optimized_config() {
    echo -e "${YELLOW}📝 Creating optimized postgresql.conf...${NC}"

    # Create backup of current config
    if [ -f "/etc/postgresql/*/main/postgresql.conf" ]; then
        CONFIG_FILE=$(find /etc/postgresql -name "postgresql.conf" | head -1)
        BACKUP_FILE="${CONFIG_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
        sudo cp "$CONFIG_FILE" "$BACKUP_FILE"
        echo -e "${GREEN}✅ Backup created: $BACKUP_FILE${NC}"
    elif [ -f "/var/lib/postgresql/*/data/postgresql.conf" ]; then
        CONFIG_FILE=$(find /var/lib/postgresql -name "postgresql.conf" | head -1)
        BACKUP_FILE="${CONFIG_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
        sudo cp "$CONFIG_FILE" "$BACKUP_FILE"
        echo -e "${GREEN}✅ Backup created: $BACKUP_FILE${NC}"
    else
        echo -e "${YELLOW}⚠️  Could not find postgresql.conf, creating template${NC}"
        CONFIG_FILE="postgresql_optimized.conf"
    fi

    # Create optimized configuration
    cat > postgresql_optimized.conf << EOF
# PostgreSQL Optimized Configuration for High Concurrency Banking Application
# Generated on $(date)

# Connection Settings
max_connections = ${RECOMMENDED_MAX_CONNECTIONS}
superuser_reserved_connections = 3

# Memory Settings
shared_buffers = ${RECOMMENDED_SHARED_BUFFERS}MB
effective_cache_size = ${RECOMMENDED_EFFECTIVE_CACHE_SIZE}MB
work_mem = ${RECOMMENDED_WORK_MEM}MB
maintenance_work_mem = ${RECOMMENDED_MAINTENANCE_WORK_MEM}MB

# Write-Ahead Logging (WAL)
wal_buffers = 16MB
checkpoint_completion_target = 0.9
wal_writer_delay = 200ms
commit_delay = 1000
commit_siblings = 5

# Query Planner
random_page_cost = 1.1
effective_io_concurrency = 200

# Background Writer
bgwriter_delay = 200ms
bgwriter_lru_maxpages = 100
bgwriter_lru_multiplier = 2.0

# Autovacuum
autovacuum = on
autovacuum_max_workers = 3
autovacuum_naptime = 1min
autovacuum_vacuum_threshold = 50
autovacuum_analyze_threshold = 50
autovacuum_vacuum_scale_factor = 0.2
autovacuum_analyze_scale_factor = 0.1

# Logging
log_destination = 'stderr'
logging_collector = on
log_directory = 'log'
log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'
log_rotation_age = 1d
log_rotation_size = 100MB
log_min_duration_statement = 1000
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on
log_temp_files = 0

# Statistics
track_activities = on
track_counts = on
track_io_timing = on
track_functions = all

# Lock Management
deadlock_timeout = 1s
lock_timeout = 30s

# Statement Timeout
statement_timeout = 30s

# Connection Pooling
max_prepared_transactions = 0

# Replication (if needed)
# max_wal_senders = 3
# wal_keep_segments = 32

# Performance Tuning
synchronous_commit = off
fsync = on
full_page_writes = on
EOF

    echo -e "${GREEN}✅ Optimized configuration created: postgresql_optimized.conf${NC}"
    echo ""
    echo -e "${YELLOW}📋 To apply these settings:${NC}"
    echo "1. Copy the optimized config to your PostgreSQL config directory:"
    echo "   sudo cp postgresql_optimized.conf $CONFIG_FILE"
    echo ""
    echo "2. Restart PostgreSQL:"
    echo "   sudo systemctl restart postgresql"
    echo ""
    echo "3. Verify the settings:"
    echo "   psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"SHOW max_connections;\""
    echo "   psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c \"SHOW shared_buffers;\""
}

# Function to apply settings via ALTER SYSTEM (requires restart)
apply_settings_via_sql() {
    echo -e "${YELLOW}🔧 Applying settings via SQL (requires restart)...${NC}"

    # Set the parameters
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME << EOF
ALTER SYSTEM SET max_connections = '${RECOMMENDED_MAX_CONNECTIONS}';
ALTER SYSTEM SET shared_buffers = '${RECOMMENDED_SHARED_BUFFERS}MB';
ALTER SYSTEM SET effective_cache_size = '${RECOMMENDED_EFFECTIVE_CACHE_SIZE}MB';
ALTER SYSTEM SET work_mem = '${RECOMMENDED_WORK_MEM}MB';
ALTER SYSTEM SET maintenance_work_mem = '${RECOMMENDED_MAINTENANCE_WORK_MEM}MB';
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET checkpoint_completion_target = '0.9';
ALTER SYSTEM SET wal_writer_delay = '200ms';
ALTER SYSTEM SET commit_delay = '1000';
ALTER SYSTEM SET commit_siblings = '5';
ALTER SYSTEM SET random_page_cost = '1.1';
ALTER SYSTEM SET effective_io_concurrency = '200';
ALTER SYSTEM SET bgwriter_delay = '200ms';
ALTER SYSTEM SET bgwriter_lru_maxpages = '100';
ALTER SYSTEM SET bgwriter_lru_multiplier = '2.0';
ALTER SYSTEM SET autovacuum = 'on';
ALTER SYSTEM SET autovacuum_max_workers = '3';
ALTER SYSTEM SET autovacuum_naptime = '1min';
ALTER SYSTEM SET autovacuum_vacuum_threshold = '50';
ALTER SYSTEM SET autovacuum_analyze_threshold = '50';
ALTER SYSTEM SET autovacuum_vacuum_scale_factor = '0.2';
ALTER SYSTEM SET autovacuum_analyze_scale_factor = '0.1';
ALTER SYSTEM SET log_min_duration_statement = '1000';
ALTER SYSTEM SET log_checkpoints = 'on';
ALTER SYSTEM SET log_connections = 'on';
ALTER SYSTEM SET log_disconnections = 'on';
ALTER SYSTEM SET log_lock_waits = 'on';
ALTER SYSTEM SET log_temp_files = '0';
ALTER SYSTEM SET track_activities = 'on';
ALTER SYSTEM SET track_counts = 'on';
ALTER SYSTEM SET track_io_timing = 'on';
ALTER SYSTEM SET track_functions = 'all';
ALTER SYSTEM SET deadlock_timeout = '1s';
ALTER SYSTEM SET lock_timeout = '30s';
ALTER SYSTEM SET statement_timeout = '30s';
ALTER SYSTEM SET synchronous_commit = 'off';
EOF

    echo -e "${GREEN}✅ Settings applied via SQL${NC}"
    echo -e "${YELLOW}⚠️  PostgreSQL restart required for changes to take effect${NC}"
}

# Function to show current connection usage
show_connection_usage() {
    echo -e "${YELLOW}📊 Current Connection Usage:${NC}"

    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT
    setting as max_connections,
    (SELECT count(*) FROM pg_stat_activity) as current_connections,
    (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_connections,
    (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle') as idle_connections,
    ROUND(
        (SELECT count(*) FROM pg_stat_activity)::numeric /
        (SELECT setting FROM pg_settings WHERE name = 'max_connections')::numeric * 100, 2
    ) as connection_usage_percent
FROM pg_settings
WHERE name = 'max_connections';
"
}

# Function to show performance statistics
show_performance_stats() {
    echo -e "${YELLOW}📈 Performance Statistics:${NC}"

    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation
FROM pg_stats
WHERE schemaname = 'public'
ORDER BY n_distinct DESC
LIMIT 10;
"
}

# Main script logic
main() {
    case "${1:-help}" in
        "check")
            check_postgresql
            get_current_settings
            show_connection_usage
            ;;
        "create-config")
            create_optimized_config
            ;;
        "apply-sql")
            check_postgresql
            apply_settings_via_sql
            ;;
        "stats")
            check_postgresql
            show_performance_stats
            ;;
        "all")
            check_postgresql
            get_current_settings
            create_optimized_config
            apply_settings_via_sql
            show_connection_usage
            ;;
        "help"|*)
            echo -e "${BLUE}Usage: $0 [command]${NC}"
            echo ""
            echo "Commands:"
            echo "  check        - Check PostgreSQL status and current settings"
            echo "  create-config - Create optimized postgresql.conf file"
            echo "  apply-sql    - Apply settings via SQL (requires restart)"
            echo "  stats        - Show performance statistics"
            echo "  all          - Run all optimizations"
            echo "  help         - Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 check"
            echo "  $0 create-config"
            echo "  $0 apply-sql"
            echo "  $0 all"
            ;;
    esac
}

# Run main function
main "$@"