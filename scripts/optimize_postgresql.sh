#!/bin/bash

# PostgreSQL Performance Optimization Script
# This script applies performance settings for bulk operations

echo "🔧 PostgreSQL Performance Optimization Script"
echo "============================================="

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then
    echo "⚠️  Warning: This script should be run with sudo for system-wide PostgreSQL settings"
    echo "   For session-only settings, continue without sudo"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# PostgreSQL configuration file location
PG_CONF_FILE="/etc/postgresql/*/main/postgresql.conf"
PG_CONF_DIR="/etc/postgresql"

# Find the actual PostgreSQL configuration file
if [ -f /etc/postgresql/*/main/postgresql.conf ]; then
    PG_CONF_FILE=$(find /etc/postgresql -name "postgresql.conf" | head -1)
    echo "📁 Found PostgreSQL config: $PG_CONF_FILE"
else
    echo "❌ PostgreSQL configuration file not found in standard location"
    echo "   Please manually edit your postgresql.conf file"
    exit 1
fi

# Backup original configuration
BACKUP_FILE="${PG_CONF_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
echo "💾 Creating backup: $BACKUP_FILE"
cp "$PG_CONF_FILE" "$BACKUP_FILE"

# Performance settings for bulk operations
echo "⚡ Applying performance settings..."

# Function to update or add setting in postgresql.conf
update_setting() {
    local setting="$1"
    local value="$2"
    local comment="$3"
    
    if grep -q "^[[:space:]]*$setting[[:space:]]*=" "$PG_CONF_FILE"; then
        # Update existing setting
        sed -i "s/^[[:space:]]*$setting[[:space:]]*=.*/$setting = $value  # $comment/" "$PG_CONF_FILE"
        echo "   ✅ Updated: $setting = $value"
    else
        # Add new setting
        echo "" >> "$PG_CONF_FILE"
        echo "# $comment" >> "$PG_CONF_FILE"
        echo "$setting = $value" >> "$PG_CONF_FILE"
        echo "   ➕ Added: $setting = $value"
    fi
}

# Apply performance settings
update_setting "synchronous_commit" "off" "Fast commits for bulk operations (can be changed at runtime)"
update_setting "full_page_writes" "off" "Disable full page writes for bulk operations (requires restart)"
update_setting "wal_buffers" "16MB" "Increase WAL buffers for better performance"
update_setting "shared_buffers" "256MB" "Increase shared buffers"
update_setting "effective_cache_size" "1GB" "Set effective cache size"
update_setting "work_mem" "4MB" "Increase work memory for bulk operations"
update_setting "maintenance_work_mem" "64MB" "Increase maintenance work memory"
update_setting "checkpoint_completion_target" "0.9" "Faster checkpoint completion"
update_setting "wal_writer_delay" "200ms" "Faster WAL writing"
update_setting "commit_delay" "0" "No commit delay for immediate commits"
update_setting "commit_siblings" "5" "Minimum concurrent transactions for commit delay"

echo ""
echo "🔧 PostgreSQL settings applied successfully!"
echo ""
echo "⚠️  IMPORTANT: Some settings require PostgreSQL restart:"
echo "   - full_page_writes = off"
echo "   - shared_buffers = 256MB"
echo "   - wal_buffers = 16MB"
echo ""
echo "🔄 To restart PostgreSQL:"
if command -v systemctl &> /dev/null; then
    echo "   sudo systemctl restart postgresql"
elif command -v service &> /dev/null; then
    echo "   sudo service postgresql restart"
else
    echo "   sudo pg_ctl restart -D /var/lib/postgresql/data"
fi
echo ""
echo "📊 Runtime-changeable settings (applied immediately):"
echo "   - synchronous_commit = off"
echo "   - work_mem = 4MB"
echo "   - maintenance_work_mem = 64MB"
echo ""
echo "💡 To apply runtime settings to current session:"
echo "   psql -c \"SET synchronous_commit = off;\""
echo ""
echo "📁 Backup created at: $BACKUP_FILE"
echo "✅ Configuration complete!" 