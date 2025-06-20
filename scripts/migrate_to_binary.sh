#!/bin/bash

# Redis Binary Migration Script
# This script helps migrate from JSON to binary serialization

set -e

echo "🚀 Starting Redis Binary Migration..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
REDIS_HOST=${REDIS_HOST:-"localhost"}
REDIS_PORT=${REDIS_PORT:-"6379"}
REDIS_PASSWORD=${REDIS_PASSWORD:-""}

echo -e "${BLUE}Configuration:${NC}"
echo "  Redis Host: $REDIS_HOST"
echo "  Redis Port: $REDIS_PORT"
echo "  Redis Password: ${REDIS_PASSWORD:+"***"}"
echo ""

# Function to check if Redis is accessible
check_redis() {
    echo -e "${BLUE}Checking Redis connectivity...${NC}"
    
    if [ -n "$REDIS_PASSWORD" ]; then
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" ping > /dev/null 2>&1
    else
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping > /dev/null 2>&1
    fi
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Redis is accessible${NC}"
    else
        echo -e "${RED}❌ Cannot connect to Redis${NC}"
        exit 1
    fi
}

# Function to backup Redis data (optional)
backup_redis() {
    echo -e "${BLUE}Creating Redis backup...${NC}"
    
    BACKUP_FILE="redis_backup_$(date +%Y%m%d_%H%M%S).rdb"
    
    if [ -n "$REDIS_PASSWORD" ]; then
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" BGSAVE
    else
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" BGSAVE
    fi
    
    echo -e "${GREEN}✅ Redis backup initiated: $BACKUP_FILE${NC}"
    echo -e "${YELLOW}⚠️  Note: Backup file location depends on Redis configuration${NC}"
}

# Function to clear Redis cache
clear_redis_cache() {
    echo -e "${BLUE}Clearing Redis cache...${NC}"
    
    # Check if we should clear all data or just specific keys
    if [ "$1" = "--all" ]; then
        echo -e "${YELLOW}⚠️  Clearing ALL Redis data...${NC}"
        read -p "Are you sure you want to clear ALL Redis data? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            if [ -n "$REDIS_PASSWORD" ]; then
                redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" FLUSHALL
            else
                redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" FLUSHALL
            fi
            echo -e "${GREEN}✅ All Redis data cleared${NC}"
        else
            echo -e "${YELLOW}⚠️  Skipping cache clear${NC}"
        fi
    else
        echo -e "${BLUE}Clearing only banking-related keys...${NC}"
        
        # Clear account keys
        if [ -n "$REDIS_PASSWORD" ]; then
            redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" --eval - <<EOF
local keys = redis.call('keys', 'account:*')
if #keys > 0 then
    return redis.call('del', unpack(keys))
else
    return 0
end
EOF
            redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" --eval - <<EOF
local keys = redis.call('keys', 'events:*')
if #keys > 0 then
    return redis.call('del', unpack(keys))
else
    return 0
end
EOF
        else
            redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --eval - <<EOF
local keys = redis.call('keys', 'account:*')
if #keys > 0 then
    return redis.call('del', unpack(keys))
else
    return 0
end
EOF
            redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --eval - <<EOF
local keys = redis.call('keys', 'events:*')
if #keys > 0 then
    return redis.call('del', unpack(keys))
else
    return 0
end
EOF
        fi
        
        echo -e "${GREEN}✅ Banking-related cache cleared${NC}"
    fi
}

# Function to build and test the application
build_and_test() {
    echo -e "${BLUE}Building application...${NC}"
    
    # Build the application
    cargo build --release
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Build successful${NC}"
    else
        echo -e "${RED}❌ Build failed${NC}"
        exit 1
    fi
    
    echo -e "${BLUE}Running tests...${NC}"
    
    # Run tests
    cargo test
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Tests passed${NC}"
    else
        echo -e "${RED}❌ Tests failed${NC}"
        exit 1
    fi
}

# Function to restart services
restart_services() {
    echo -e "${BLUE}Restarting services...${NC}"
    
    # This is a placeholder - adjust based on your deployment setup
    echo -e "${YELLOW}⚠️  Please restart your application services manually${NC}"
    echo -e "${YELLOW}   - Stop current application instances${NC}"
    echo -e "${YELLOW}   - Deploy new binary${NC}"
    echo -e "${YELLOW}   - Start application instances${NC}"
}

# Function to verify migration
verify_migration() {
    echo -e "${BLUE}Verifying migration...${NC}"
    
    # Check if new data is being written in binary format
    echo -e "${YELLOW}⚠️  Please verify manually:${NC}"
    echo -e "${YELLOW}   1. Check Redis for new binary data${NC}"
    echo -e "${YELLOW}   2. Verify cache operations work correctly${NC}"
    echo -e "${YELLOW}   3. Test Kafka message production/consumption${NC}"
    echo -e "${YELLOW}   4. Monitor application logs for errors${NC}"
}

# Main execution
main() {
    echo -e "${BLUE}=== Redis Binary Migration Script ===${NC}"
    echo ""
    
    # Check Redis connectivity
    check_redis
    
    # Ask user what they want to do
    echo -e "${BLUE}Migration Options:${NC}"
    echo "1. Full migration (backup + clear all + build + restart)"
    echo "2. Safe migration (backup + clear banking keys + build + restart)"
    echo "3. Build and test only"
    echo "4. Clear cache only"
    echo "5. Exit"
    
    read -p "Choose an option (1-5): " choice
    
    case $choice in
        1)
            echo -e "${BLUE}Starting full migration...${NC}"
            backup_redis
            clear_redis_cache --all
            build_and_test
            restart_services
            verify_migration
            ;;
        2)
            echo -e "${BLUE}Starting safe migration...${NC}"
            backup_redis
            clear_redis_cache
            build_and_test
            restart_services
            verify_migration
            ;;
        3)
            echo -e "${BLUE}Building and testing only...${NC}"
            build_and_test
            ;;
        4)
            echo -e "${BLUE}Clearing cache only...${NC}"
            clear_redis_cache
            ;;
        5)
            echo -e "${BLUE}Exiting...${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid option${NC}"
            exit 1
            ;;
    esac
    
    echo ""
    echo -e "${GREEN}🎉 Migration completed!${NC}"
    echo -e "${BLUE}Remember to monitor your application for any issues.${NC}"
}

# Run main function
main "$@" 