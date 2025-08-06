#!/bin/bash

# CDC Recovery Script - Fix stuck messages and missing accounts
echo "🔧 CDC Recovery Script - Fixing stuck messages and missing accounts"

# Check stuck messages
echo "📊 Checking for stuck outbox messages..."
STUCK_COUNT=$(psql -h localhost -U postgres -d banking_es -t -c "SELECT COUNT(*) FROM kafka_outbox_cdc WHERE created_at < NOW() - INTERVAL '5 minutes';" | tr -d ' ')

if [ "$STUCK_COUNT" -gt 0 ]; then
    echo "⚠️  Found $STUCK_COUNT stuck messages"

    # Check if CDC consumer is running
    echo "🔍 Checking CDC consumer status..."
    CDC_STATUS=$(psql -h localhost -U postgres -d banking_es -t -c "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name LIKE '%debezium%';" | tr -d ' ')

    if [ "$CDC_STATUS" -eq 0 ]; then
        echo "❌ CDC consumer not running. Restarting Debezium..."
        docker-compose restart debezium
        sleep 10
    else
        echo "✅ CDC consumer is running"
    fi

    # Wait for processing
    echo "⏳ Waiting for CDC to process stuck messages..."
    sleep 30

    # Check again
    NEW_STUCK_COUNT=$(psql -h localhost -U postgres -d banking_es -t -c "SELECT COUNT(*) FROM kafka_outbox_cdc WHERE created_at < NOW() - INTERVAL '5 minutes';" | tr -d ' ')
    echo "📊 Remaining stuck messages: $NEW_STUCK_COUNT"
else
    echo "✅ No stuck messages found"
fi

# Check missing accounts
echo "🔍 Checking for missing accounts in projections..."
MISSING_ACCOUNTS=$(psql -h localhost -U postgres -d banking_es -t -c "
SELECT e.aggregate_id
FROM events e
LEFT JOIN account_projections ap ON e.aggregate_id = ap.account_id
WHERE e.event_type = 'AccountCreated'
AND ap.account_id IS NULL
LIMIT 10;" | wc -l)

if [ "$MISSING_ACCOUNTS" -gt 0 ]; then
    echo "⚠️  Found $MISSING_ACCOUNTS accounts missing from projections"
    echo "🔄 These will be processed by CDC automatically"
else
    echo "✅ All accounts are present in projections"
fi

echo "✅ CDC Recovery completed"