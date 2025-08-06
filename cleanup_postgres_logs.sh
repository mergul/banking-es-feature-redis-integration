#!/bin/bash

# PostgreSQL Log Cleanup Script
# This script safely removes old PostgreSQL log files

LOG_DIR="/var/lib/postgresql/17/main/log"
DAYS_TO_KEEP=1

echo "PostgreSQL Log Cleanup Script"
echo "============================="
echo "Log directory: $LOG_DIR"
echo "Keeping logs from last $DAYS_TO_KEEP days"
echo ""

# Check if we're running as root
if [ "$EUID" -ne 0 ]; then
    echo "This script must be run as root (use sudo)"
    exit 1
fi

# Check if log directory exists
if [ ! -d "$LOG_DIR" ]; then
    echo "Error: Log directory $LOG_DIR does not exist"
    exit 1
fi

# Show current log directory size
echo "Current log directory size:"
du -sh "$LOG_DIR"
echo ""

# Count files to be deleted
echo "Files older than $DAYS_TO_KEEP days:"
OLD_FILES=$(find "$LOG_DIR" -name "*.log" -mtime +$DAYS_TO_KEEP)
if [ -z "$OLD_FILES" ]; then
    echo "No old files found to delete"
    exit 0
fi

echo "$OLD_FILES" | wc -l
echo ""

# Calculate space to be freed
SPACE_TO_FREE=$(find "$LOG_DIR" -name "*.log" -mtime +$DAYS_TO_KEEP -exec du -ch {} + | tail -1 | cut -f1)
echo "Space to be freed: $SPACE_TO_FREE"
echo ""

# Ask for confirmation
read -p "Do you want to proceed with deletion? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Operation cancelled"
    exit 0
fi

# Delete old files
echo "Deleting old log files..."
find "$LOG_DIR" -name "*.log" -mtime +$DAYS_TO_KEEP -delete

# Show new directory size
echo ""
echo "Log directory size after cleanup:"
du -sh "$LOG_DIR"

echo ""
echo "Cleanup completed successfully!"