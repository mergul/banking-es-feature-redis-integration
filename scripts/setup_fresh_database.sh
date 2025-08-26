#!/bin/bash

# Fresh Database Setup Script
# This script resets the database and sets up a fresh environment

set -e

echo "🚀 Starting fresh database setup..."

# Step 1: Reset database (drop all objects)
echo "📝 Step 1: Resetting database..."
psql -h localhost -U postgres -d banking_es -f scripts/reset_database.sql

# Step 2: Create tables in the correct order (matching migrations)
echo "📦 Step 2: Creating tables..."

# Create events table (includes snapshots table)
echo "  - Creating events table and snapshots..."
psql -h localhost -U postgres -d banking_es -f scripts/create_events_table.sql

# Create account_projections table
echo "  - Creating account_projections table..."
psql -h localhost -U postgres -d banking_es -f scripts/create_accounts_table.sql

# Create transaction_projections table
echo "  - Creating transaction_projections table..."
psql -h localhost -U postgres -d banking_es -f scripts/create_transactions_table.sql

# Create users table
echo "  - Creating users table..."
psql -h localhost -U postgres -d banking_es -f scripts/create_users_table.sql

# Create outbox tables (only kafka_outbox_cdc)
echo "  - Creating outbox tables..."
psql -h localhost -U postgres -d banking_es -f scripts/create_outbox_tables.sql

echo "✅ Fresh database setup completed successfully!"
echo ""
echo "📊 Database Summary:"
echo "  - Events table (hash partitioned) + Snapshots table"
echo "  - Account_projections table (version column dropped)"
echo "  - Transaction_projections table (partitioned)"
echo "  - Users table"
echo "  - Kafka_outbox_cdc table (CDC optimized + cleanup functions)"
echo ""
echo "🎯 Ready for development!" 