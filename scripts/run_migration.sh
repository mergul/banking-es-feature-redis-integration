#!/bin/bash

# Script to run the database migration for adding version column to account_projections

set -e

echo "Running migration to add version column to account_projections..."

# Check if DATABASE_URL is set
if [ -z "$DATABASE_URL" ]; then
    echo "Error: DATABASE_URL environment variable is not set"
    echo "Please set it to your PostgreSQL connection string"
    echo "Example: export DATABASE_URL='postgresql://username:password@localhost:5432/database_name'"
    exit 1
fi

# Run the migration
echo "Executing migration..."
psql "$DATABASE_URL" -f migrations/20241201000001_add_version_to_account_projections.sql

echo "Migration completed successfully!"
echo "The version column has been added to the account_projections table."
echo "This enables optimistic concurrency control and proper event ordering." 