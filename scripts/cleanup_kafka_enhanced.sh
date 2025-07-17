#!/bin/bash

# Enhanced Kafka Cleanup Script for Banking ES
# This script provides comprehensive cleanup options for Kafka topics

set -e

# Configuration
KAFKA_BIN="${KAFKA_BIN:-/home/kafka/kafka/bin}"
BROKER="${BROKER:-localhost:9092}"
CONSUMER_GROUP="${CONSUMER_GROUP:-banking-es-group}"

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

# Function to check if Kafka is running
check_kafka() {
    log_info "Checking Kafka connectivity..."
    if ! "$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BROKER" >/dev/null 2>&1; then
        log_error "Cannot connect to Kafka at $BROKER"
        log_error "Please ensure Kafka is running and accessible"
        exit 1
    fi
    log_success "Kafka is accessible at $BROKER"
}

# Function to list all topics
list_topics() {
    log_info "Listing all topics..."
    "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKER" --list
}

# Function to get topic details
get_topic_details() {
    local topic="$1"
    log_debug "Getting details for topic: $topic"
    "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKER" --topic "$topic" --describe 2>/dev/null || {
        log_warning "Topic $topic does not exist or is not accessible"
        return 1
    }
}

# Function to get topic partitions
get_topic_partitions() {
    local topic="$1"
    "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKER" --topic "$topic" --describe 2>/dev/null | grep -Eo 'Partition: [0-9]+' | awk '{print $2}' || echo ""
}

# Function to show topic status
show_topic_status() {
    local topic="$1"
    log_info "Topic: $topic"
    
    if get_topic_details "$topic" >/dev/null 2>&1; then
        local partitions
        partitions=$(get_topic_partitions "$topic")
        if [ -n "$partitions" ]; then
            local partition_count
            partition_count=$(echo "$partitions" | wc -l)
            log_info "  Partitions: $partition_count"
            
            # Get message count for each partition
            for partition in $partitions; do
                local offset
                offset=$("$KAFKA_BIN/kafka-run-class.sh" kafka.tools.GetOffsetShell --bootstrap-server "$BROKER" --topic "$topic" --partition "$partition" --time -1 2>/dev/null | cut -d: -f3 || echo "0")
                log_info "    Partition $partition: $offset messages"
            done
        fi
    else
        log_warning "  Topic does not exist"
    fi
}

# Function to show all topics status
show_all_topics_status() {
    log_step "Kafka Topics Status"
    
    local banking_topics=(
        "banking-es.public.kafka_outbox_cdc"
        "banking-es-events"
        "banking-es-cache"
        "banking-es-dlq"
        "banking-es-cache-invalidation"
        "schema-changes.banking-es"
    )
    
    for topic in "${banking_topics[@]}"; do
        show_topic_status "$topic"
        echo ""
    done
}

# Function to delete records from a topic
delete_topic_records() {
    local topic="$1"
    local description="$2"
    
    log_info "Deleting records from topic: $topic"
    
    local partitions
    partitions=$(get_topic_partitions "$topic")
    
    if [ -z "$partitions" ]; then
        log_warning "No partitions found for topic $topic"
        return
    fi
    
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
        log_success "Deleted records from $topic"
    else
        log_error "Failed to delete records from $topic"
    fi
    
    # Clean up temporary file
    rm -f "$tmp_json"
}

# Function to delete a topic completely
delete_topic() {
    local topic="$1"
    
    log_warning "Deleting topic completely: $topic"
    
    if "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKER" --topic "$topic" --delete >/dev/null 2>&1; then
        log_success "Deleted topic: $topic"
    else
        log_error "Failed to delete topic: $topic"
    fi
}

# Function to reset consumer group offsets
reset_consumer_group() {
    local group="$1"
    
    log_info "Resetting consumer group: $group"
    
    # Get topics for the consumer group
    local topics
    topics=$("$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BROKER" --group "$group" --describe 2>/dev/null | grep -v "TOPIC" | awk '{print $1}' | sort -u || echo "")
    
    if [ -z "$topics" ]; then
        log_warning "No topics found for consumer group $group"
        return
    fi
    
    # Reset offsets for each topic
    for topic in $topics; do
        log_info "Resetting offsets for topic: $topic"
        "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BROKER" --group "$group" --topic "$topic" --reset-offsets --to-earliest --execute >/dev/null 2>&1 || {
            log_error "Failed to reset offsets for topic $topic"
        }
    done
    
    log_success "Reset consumer group: $group"
}

# Function to cleanup CDC topics
cleanup_cdc_topics() {
    log_step "Cleaning up CDC Topics"
    
    local cdc_topics=(
        "banking-es.public.kafka_outbox_cdc"
        "schema-changes.banking-es"
    )
    
    for topic in "${cdc_topics[@]}"; do
        delete_topic_records "$topic" "CDC topic"
    done
}

# Function to cleanup event topics
cleanup_event_topics() {
    log_step "Cleaning up Event Topics"
    
    local event_topics=(
        "banking-es-events"
        "banking-es-cache"
        "banking-es-cache-invalidation"
    )
    
    for topic in "${event_topics[@]}"; do
        delete_topic_records "$topic" "Event topic"
    done
}

# Function to cleanup DLQ topic
cleanup_dlq_topic() {
    log_step "Cleaning up DLQ Topic"
    
    delete_topic_records "banking-es-dlq" "DLQ topic"
}

# Function to cleanup consumer groups
cleanup_consumer_groups() {
    log_step "Cleaning up Consumer Groups"
    
    local consumer_groups=(
        "banking-es-group"
        "banking-es-dlq-recovery"
    )
    
    for group in "${consumer_groups[@]}"; do
        reset_consumer_group "$group"
    done
}

# Function to show consumer group status
show_consumer_group_status() {
    local group="$1"
    log_info "Consumer Group: $group"
    
    if "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BROKER" --group "$group" --describe >/dev/null 2>&1; then
        "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BROKER" --group "$group" --describe | head -20
    else
        log_warning "Consumer group $group does not exist or has no active members"
    fi
}

# Function to show all consumer groups status
show_all_consumer_groups_status() {
    log_step "Consumer Groups Status"
    
    local consumer_groups=(
        "banking-es-group"
        "banking-es-dlq-recovery"
    )
    
    for group in "${consumer_groups[@]}"; do
        show_consumer_group_status "$group"
        echo ""
    done
}

# Main cleanup function
cleanup_kafka() {
    log_step "Starting Enhanced Kafka Cleanup"
    
    # Check Kafka connectivity
    check_kafka
    
    # Show initial status
    show_all_topics_status
    show_all_consumer_groups_status
    
    # Confirm cleanup
    log_warning "This will DELETE ALL MESSAGES from Kafka topics!"
    read -p "Are you absolutely sure you want to continue? Type 'YES' to confirm: " -r
    if [ "$REPLY" != "YES" ]; then
        log_info "Cleanup cancelled by user."
        exit 0
    fi
    
    # Perform cleanup operations
    cleanup_cdc_topics
    cleanup_event_topics
    cleanup_dlq_topic
    cleanup_consumer_groups
    
    # Show final status
    log_step "Final Kafka Status"
    show_all_topics_status
    show_all_consumer_groups_status
    
    log_success "Enhanced Kafka cleanup completed successfully!"
}

# Function to delete all topics (nuclear option)
delete_all_topics() {
    log_warning "NUCLEAR OPTION: This will DELETE ALL TOPICS!"
    read -p "Are you absolutely sure? Type 'DELETE ALL' to confirm: " -r
    if [ "$REPLY" != "DELETE ALL" ]; then
        log_info "Nuclear cleanup cancelled by user."
        exit 0
    fi
    
    log_step "Deleting all topics..."
    
    # Get all topics
    local all_topics
    all_topics=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BROKER" --list 2>/dev/null || echo "")
    
    if [ -n "$all_topics" ]; then
        echo "$all_topics" | while read -r topic; do
            if [ -n "$topic" ]; then
                delete_topic "$topic"
            fi
        done
    fi
    
    log_success "All topics deleted!"
}

# Function to show help
show_help() {
    echo "Enhanced Kafka Cleanup Script"
    echo ""
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  cleanup    - Clean up all messages from topics (delete records)"
    echo "  delete-all - Nuclear option: delete all topics completely"
    echo "  status     - Show current Kafka status (topics and consumer groups)"
    echo "  topics     - Show only topics status"
    echo "  consumers  - Show only consumer groups status"
    echo "  help       - Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  KAFKA_BIN      - Kafka bin directory (default: /home/kafka/kafka/bin)"
    echo "  BROKER         - Kafka broker address (default: localhost:9092)"
    echo "  CONSUMER_GROUP - Consumer group name (default: banking-es-group)"
    echo ""
}

# Main script logic
case "${1:-cleanup}" in
    "cleanup")
        cleanup_kafka
        ;;
    "delete-all")
        delete_all_topics
        ;;
    "status")
        check_kafka
        show_all_topics_status
        show_all_consumer_groups_status
        ;;
    "topics")
        check_kafka
        show_all_topics_status
        ;;
    "consumers")
        check_kafka
        show_all_consumer_groups_status
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