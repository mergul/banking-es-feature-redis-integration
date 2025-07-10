#!/bin/bash

# Kafka Log Cleanup Script
# This script cleans up old Kafka log files

KAFKA_LOGS_DIR="/home/kafka/kafka/logs"
KAFKA_DATA_LOGS_DIR="/home/kafka/logs"
DAYS_TO_KEEP=7

echo "Kafka Log Cleanup Script"
echo "========================"
echo "Date: $(date)"
echo "Kafka logs directory: $KAFKA_LOGS_DIR"
echo "Kafka data logs directory: $KAFKA_DATA_LOGS_DIR"
echo "Keeping logs from last $DAYS_TO_KEEP days"
echo ""

# Check if we're running as root or kafka user
if [ "$EUID" -ne 0 ] && [ "$USER" != "kafka" ]; then
    echo "This script must be run as root (use sudo) or kafka user"
    exit 1
fi

# Function to clean up log files
cleanup_logs() {
    local log_dir="$1"
    local dir_name="$2"
    
    if [ ! -d "$log_dir" ]; then
        echo "Warning: Log directory $log_dir does not exist"
        return
    fi
    
    echo "Cleaning up $dir_name..."
    echo "Current size: $(du -sh "$log_dir" | cut -f1)"
    
    # Remove old log files
    OLD_FILES=$(find "$log_dir" -name "*.log*" -mtime +$DAYS_TO_KEEP 2>/dev/null)
    if [ -n "$OLD_FILES" ]; then
        echo "Found $(echo "$OLD_FILES" | wc -l) old files to remove"
        find "$log_dir" -name "*.log*" -mtime +$DAYS_TO_KEEP -delete 2>/dev/null
        echo "Old files removed."
    else
        echo "No old files found to remove."
    fi
    
    echo "Size after cleanup: $(du -sh "$log_dir" | cut -f1)"
    echo ""
}

# Clean up Kafka server logs
cleanup_logs "$KAFKA_LOGS_DIR" "Kafka server logs"

# Clean up Kafka data logs (these are topic data, be more careful)
echo "Checking Kafka data logs directory..."
if [ -d "$KAFKA_DATA_LOGS_DIR" ]; then
    echo "Current size: $(du -sh "$KAFKA_DATA_LOGS_DIR" | cut -f1)"
    echo "Note: This directory contains topic data. Only removing very old files (>30 days)."
    
    # Be more conservative with data logs - only remove very old files
    OLD_DATA_FILES=$(find "$KAFKA_DATA_LOGS_DIR" -name "*.log*" -mtime +30 2>/dev/null)
    if [ -n "$OLD_DATA_FILES" ]; then
        echo "Found $(echo "$OLD_DATA_FILES" | wc -l) very old data files to remove"
        find "$KAFKA_DATA_LOGS_DIR" -name "*.log*" -mtime +30 -delete 2>/dev/null
        echo "Very old data files removed."
    else
        echo "No very old data files found to remove."
    fi
    
    echo "Size after cleanup: $(du -sh "$KAFKA_DATA_LOGS_DIR" | cut -f1)"
else
    echo "Warning: Kafka data logs directory $KAFKA_DATA_LOGS_DIR does not exist"
fi

echo ""
echo "Kafka log cleanup completed!" 