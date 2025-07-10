#!/bin/bash

# PostgreSQL Log Rotation Script
# This script should be run daily via cron to manage log files

LOG_DIR="/var/lib/postgresql/17/main/log"
DAYS_TO_KEEP=7
MAX_LOG_SIZE="100M"

echo "PostgreSQL Log Rotation Script"
echo "=============================="
echo "Date: $(date)"
echo "Log directory: $LOG_DIR"
echo "Keeping logs from last $DAYS_TO_KEEP days"
echo "Max log size: $MAX_LOG_SIZE"
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

# Remove old log files (older than DAYS_TO_KEEP)
echo "Removing log files older than $DAYS_TO_KEEP days..."
OLD_FILES=$(find "$LOG_DIR" -name "*.log" -mtime +$DAYS_TO_KEEP)
if [ -n "$OLD_FILES" ]; then
    echo "Found $(echo "$OLD_FILES" | wc -l) old files to remove:"
    echo "$OLD_FILES" | head -5
    if [ $(echo "$OLD_FILES" | wc -l) -gt 5 ]; then
        echo "... and $(($(echo "$OLD_FILES" | wc -l) - 5)) more files"
    fi
    find "$LOG_DIR" -name "*.log" -mtime +$DAYS_TO_KEEP -delete
    echo "Old files removed."
else
    echo "No old files found to remove."
fi

# Compress large log files
echo ""
echo "Compressing large log files..."
LARGE_FILES=$(find "$LOG_DIR" -name "*.log" -size +$MAX_LOG_SIZE)
if [ -n "$LARGE_FILES" ]; then
    echo "Found $(echo "$LARGE_FILES" | wc -l) large files to compress:"
    echo "$LARGE_FILES" | head -5
    if [ $(echo "$LARGE_FILES" | wc -l) -gt 5 ]; then
        echo "... and $(($(echo "$LARGE_FILES" | wc -l) - 5)) more files"
    fi
    
    for file in $LARGE_FILES; do
        if [ -f "$file" ]; then
            echo "Compressing: $file"
            gzip "$file"
        fi
    done
    echo "Large files compressed."
else
    echo "No large files found to compress."
fi

# Show final log directory size
echo ""
echo "Log directory size after cleanup:"
du -sh "$LOG_DIR"

# Show file count
echo ""
echo "File count by type:"
echo "Log files: $(find "$LOG_DIR" -name "*.log" | wc -l)"
echo "Compressed files: $(find "$LOG_DIR" -name "*.log.gz" | wc -l)"
echo "Total files: $(find "$LOG_DIR" -type f | wc -l)"

echo ""
echo "Log rotation completed successfully!" 