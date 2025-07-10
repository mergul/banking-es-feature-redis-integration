#!/bin/bash

# Advanced /usr Directory Cleanup Script
# This script removes large unused applications and libraries

echo "Advanced USR Directory Cleanup Script"
echo "====================================="
echo "Date: $(date)"
echo ""

# Check if we're running as root
if [ "$EUID" -ne 0 ]; then
    echo "This script must be run as root (use sudo)"
    exit 1
fi

# Show current /usr size
echo "Current /usr directory size:"
du -sh /usr
echo ""

# Function to check if a process is running
is_running() {
    local process_name="$1"
    if pgrep -f "$process_name" > /dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Function to check if a command exists
command_exists() {
    local command="$1"
    if command -v "$command" > /dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Function to safely remove directory if not in use
safe_remove() {
    local dir="$1"
    local description="$2"
    
    if [ -d "$dir" ]; then
        echo "Checking: $description ($dir)"
        echo "Size: $(du -sh "$dir" | cut -f1)"
        
        # Check if it's safe to remove
        local safe_to_remove=true
        
        case "$description" in
            "Hazelcast")
                if is_running "hazelcast"; then
                    echo "  ⚠️  Hazelcast is running - keeping"
                    safe_to_remove=false
                fi
                ;;
            "Slack")
                if is_running "slack"; then
                    echo "  ⚠️  Slack is running - keeping"
                    safe_to_remove=false
                fi
                ;;
            "Firefox")
                if is_running "firefox"; then
                    echo "  ⚠️  Firefox is running - keeping"
                    safe_to_remove=false
                fi
                ;;
            "Thunderbird")
                if is_running "thunderbird"; then
                    echo "  ⚠️  Thunderbird is running - keeping"
                    safe_to_remove=false
                fi
                ;;
            "LibreOffice")
                if is_running "libreoffice"; then
                    echo "  ⚠️  LibreOffice is running - keeping"
                    safe_to_remove=false
                fi
                ;;
            ".NET")
                if command_exists "dotnet"; then
                    echo "  ⚠️  .NET is installed and available - keeping"
                    safe_to_remove=false
                fi
                ;;
        esac
        
        if [ "$safe_to_remove" = true ]; then
            echo "  ✅ Safe to remove - removing..."
            rm -rf "$dir"
            echo "  ✅ Removed successfully"
        fi
        echo ""
    fi
}

# Clean up large applications
echo "Cleaning up large applications..."

# Remove Hazelcast if not running (498MB + 253MB = 751MB)
safe_remove "/usr/lib/hazelcast" "Hazelcast"
safe_remove "/usr/lib/hazelcast-management-center" "Hazelcast Management Center"

# Remove Slack if not running (290MB)
safe_remove "/usr/lib/slack" "Slack"

# Remove Firefox if not running (265MB)
safe_remove "/usr/lib/firefox" "Firefox"

# Remove Thunderbird if not running (258MB)
safe_remove "/usr/lib/thunderbird" "Thunderbird"

# Remove LibreOffice if not running (318MB)
safe_remove "/usr/lib/libreoffice" "LibreOffice"

# Remove .NET if not needed (540MB)
safe_remove "/usr/lib/dotnet" ".NET"

# Clean up old firmware (keep only current kernel firmware)
echo "Cleaning up old firmware..."
if [ -d "/usr/lib/firmware" ]; then
    echo "Current firmware size: $(du -sh /usr/lib/firmware | cut -f1)"
    
    # Remove old kernel firmware
    OLD_FIRMWARE=$(find /usr/lib/firmware -name "*-generic" -type d 2>/dev/null | grep -v "$(uname -r)")
    if [ -n "$OLD_FIRMWARE" ]; then
        echo "Found old kernel firmware to remove:"
        echo "$OLD_FIRMWARE"
        for fw in $OLD_FIRMWARE; do
            echo "Removing: $fw"
            rm -rf "$fw"
        done
        echo "Old firmware removed."
    else
        echo "No old firmware found to remove."
    fi
    echo ""
fi

# Clean up old LLVM versions (keep only the latest)
echo "Cleaning up old LLVM versions..."
LLVM_DIRS=$(ls -d /usr/lib/llvm-* 2>/dev/null | sort -V)
if [ -n "$LLVM_DIRS" ] && [ $(echo "$LLVM_DIRS" | wc -l) -gt 1 ]; then
    # Keep only the latest LLVM version
    LATEST_LLVM=$(echo "$LLVM_DIRS" | tail -1)
    OLD_LLVM_DIRS=$(echo "$LLVM_DIRS" | grep -v "$LATEST_LLVM")
    
    if [ -n "$OLD_LLVM_DIRS" ]; then
        echo "Found old LLVM versions to remove:"
        echo "$OLD_LLVM_DIRS"
        for llvm in $OLD_LLVM_DIRS; do
            echo "Removing: $llvm"
            rm -rf "$llvm"
        done
        echo "Old LLVM versions removed."
    fi
else
    echo "No old LLVM versions found to remove."
fi
echo ""

# Show final /usr size
echo "Final /usr directory size:"
du -sh /usr

echo ""
echo "Advanced cleanup completed successfully!" 