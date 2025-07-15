use crate::infrastructure::cache_service::CacheServiceTrait;
use crate::infrastructure::kafka_abstraction::KafkaProducerTrait;
use crate::infrastructure::projections::ProjectionStoreTrait;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock, Semaphore};
use tokio::time::Instant;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

// Enhanced CDC-based outbox message structure with optimization fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CDCOutboxMessage {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub event_id: Uuid,
    pub event_type: String,
    pub topic: String,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>, // For optimal Kafka partitioning
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<i32>, // For schema evolution
}

/// Enhanced Debezium configuration with performance optimizations
#[derive(Debug, Clone)]
pub struct OptimizedDebeziumConfig {
    pub connector_name: String,
    pub database_host: String,
    pub database_port: u16,
    pub database_name: String,
    pub database_user: String,
    pub database_password: String,
    pub table_include_list: String,
    pub topic_prefix: String,
    pub snapshot_mode: String,
    pub poll_interval_ms: u64,

    // Performance optimizations
    pub max_batch_size: i32,
    pub max_queue_size: i32,
    pub heartbeat_interval_ms: u64,
    pub publication_autocreate_mode: String,
    pub slot_drop_on_stop: bool,
    pub include_unchanged_toasts: bool,
    pub binary_handling_mode: String,
    pub decimal_handling_mode: String,
    pub time_precision_mode: String,

    // Reliability configurations
    pub slot_retry_delay_ms: u64,
    pub max_retries: i32,
    pub retriable_restart_wait_ms: u64,
    pub flush_lsn_threshold: i64,
    pub status_update_interval_ms: u64,

    // Monitoring and observability
    pub provide_transaction_metadata: bool,
    pub tombstones_on_delete: bool,
    pub message_key_columns: Option<String>,
}

impl Default for OptimizedDebeziumConfig {
    fn default() -> Self {
        Self {
            connector_name: "banking-es-connector".to_string(),
            database_host: "localhost".to_string(),
            database_port: 5432,
            database_name: "banking_es".to_string(),
            database_user: "postgres".to_string(),
            database_password: "Francisco1".to_string(),
            table_include_list: "public.kafka_outbox_cdc".to_string(),
            topic_prefix: "banking-es".to_string(),
            snapshot_mode: "initial".to_string(),
            poll_interval_ms: 50, // Reduced for better latency

            // Performance optimizations
            max_batch_size: 2048,
            max_queue_size: 8192,
            heartbeat_interval_ms: 30000,
            publication_autocreate_mode: "filtered".to_string(),
            slot_drop_on_stop: false, // Keep slot for reliability
            include_unchanged_toasts: false,
            binary_handling_mode: "base64".to_string(),
            decimal_handling_mode: "string".to_string(),
            time_precision_mode: "adaptive".to_string(),

            // Reliability configurations
            slot_retry_delay_ms: 10000,
            max_retries: 3,
            retriable_restart_wait_ms: 10000,
            flush_lsn_threshold: 1024 * 1024, // 1MB
            status_update_interval_ms: 10000,

            // Monitoring and observability
            provide_transaction_metadata: true,
            tombstones_on_delete: false,
            message_key_columns: Some("aggregate_id".to_string()),
        }
    }
}

/// Optimized CDC-based outbox repository with connection pooling and batch operations
#[derive(Clone)]
pub struct OptimizedCDCOutboxRepository {
    pool: sqlx::PgPool,
    write_semaphore: Arc<Semaphore>, // Limit concurrent writes
    batch_cache: Arc<RwLock<HashMap<Uuid, CDCOutboxMessage>>>, // Cache for batching
    batch_size: usize,
    batch_timeout: Duration,
}

impl OptimizedCDCOutboxRepository {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            write_semaphore: Arc::new(Semaphore::new(10)), // Max 10 concurrent writes
            batch_cache: Arc::new(RwLock::new(HashMap::new())),
            batch_size: 100,
            batch_timeout: Duration::from_millis(50),
        }
    }

    /// Create optimized outbox table with better indexing and partitioning
    pub async fn create_optimized_cdc_outbox_table(&self) -> Result<()> {
        // Create the main table with optimized structure
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS kafka_outbox_cdc (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                aggregate_id UUID NOT NULL,
                event_id UUID NOT NULL UNIQUE,
                event_type VARCHAR(255) NOT NULL,
                payload BYTEA NOT NULL,
                topic VARCHAR(255) NOT NULL,
                metadata JSONB,
                partition_key VARCHAR(255),
                schema_version INTEGER DEFAULT 1,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create optimized indexes
        let indexes = vec![
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_cdc_created_at_btree ON kafka_outbox_cdc USING btree(created_at)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_cdc_aggregate_id_hash ON kafka_outbox_cdc USING hash(aggregate_id)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_cdc_event_type_hash ON kafka_outbox_cdc USING hash(event_type)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_cdc_topic_hash ON kafka_outbox_cdc USING hash(topic)",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_cdc_partition_key ON kafka_outbox_cdc(partition_key) WHERE partition_key IS NOT NULL",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_cdc_composite ON kafka_outbox_cdc(aggregate_id, created_at DESC)",
        ];

        for index_sql in indexes {
            if let Err(e) = sqlx::query(index_sql).execute(&self.pool).await {
                warn!("Failed to create index: {} - {}", index_sql, e);
            }
        }

        // Enable logical replication with optimized settings
        sqlx::query("ALTER TABLE kafka_outbox_cdc REPLICA IDENTITY FULL")
            .execute(&self.pool)
            .await?;

        // Create partition function for better performance (optional)
        sqlx::query(
            r#"
            CREATE OR REPLACE FUNCTION kafka_outbox_cdc_partition_by_day()
            RETURNS trigger AS $$
            BEGIN
                -- Future: Add partitioning logic here if needed
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            "#,
        )
        .execute(&self.pool)
        .await?;

        info!("Optimized CDC outbox table created successfully");
        Ok(())
    }

    /// Generate optimized Debezium connector configuration
    pub fn generate_optimized_debezium_config(
        &self,
        config: &OptimizedDebeziumConfig,
    ) -> serde_json::Value {
        serde_json::json!({
            "name": config.connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

                // Database connection settings
                "database.hostname": config.database_host,
                "database.port": config.database_port,
                "database.user": config.database_user,
                "database.password": config.database_password,
                "database.dbname": config.database_name,
                "database.server.name": "banking_es_server",

                // Table and topic configuration
                "table.include.list": config.table_include_list,
                "topic.prefix": config.topic_prefix,
                "message.key.columns": config.message_key_columns,

                // Snapshot configuration
                "snapshot.mode": config.snapshot_mode,
                "snapshot.include.collection.list": config.table_include_list,
                "snapshot.lock.timeout.ms": 10000,

                // Performance optimizations
                "poll.interval.ms": config.poll_interval_ms,
                "max.batch.size": config.max_batch_size,
                "max.queue.size": config.max_queue_size,
                "provide.transaction.metadata": config.provide_transaction_metadata,

                // Replication slot configuration
                "publication.autocreate.mode": config.publication_autocreate_mode,
                "slot.name": "banking_outbox_slot_v2",
                "slot.drop.on.stop": config.slot_drop_on_stop,
                "slot.retry.delay.ms": config.slot_retry_delay_ms,
                "slot.max.retries": config.max_retries,
                "plugin.name": "pgoutput",

                // Data type handling
                "binary.handling.mode": config.binary_handling_mode,
                "decimal.handling.mode": config.decimal_handling_mode,
                "time.precision.mode": config.time_precision_mode,
                "include.unchanged.toasts": config.include_unchanged_toasts,

                // Reliability settings
                "retriable.restart.connector.wait.ms": config.retriable_restart_wait_ms,
                "heartbeat.interval.ms": config.heartbeat_interval_ms,
                "status.update.interval.ms": config.status_update_interval_ms,
                "flush.lsn.threshold": config.flush_lsn_threshold,

                // Kafka Connect converters
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": false,
                "header.converter": "org.apache.kafka.connect.storage.StringConverter",

                // Transformations
                "transforms": "unwrap,addHeaders,router",
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.drop.tombstones": config.tombstones_on_delete,
                "transforms.unwrap.delete.handling.mode": "rewrite",
                "transforms.unwrap.operation.header": true,

                // Add custom headers for better routing
                "transforms.addHeaders.type": "org.apache.kafka.connect.transforms.InsertHeader",
                "transforms.addHeaders.header.aggregate_type": "account",
                "transforms.addHeaders.header.source_system": "banking-es",

                // Optional: Route to different topics based on event type
                "transforms.router.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.router.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
                "transforms.router.replacement": "$1.$2.$3",

                // Monitoring and metrics
                "errors.tolerance": "none",
                "errors.deadletterqueue.topic.name": "banking-es-dlq",
                "errors.deadletterqueue.context.headers.enable": true,
                "errors.log.enable": true,
                "errors.log.include.messages": true,

                // Security (if needed)
                "database.sslmode": "prefer",
                "database.sslcert": "",
                "database.sslkey": "",
                "database.sslrootcert": "",

                // Additional optimizations
                "skipped.operations": "none",
                "tombstones.on.delete": config.tombstones_on_delete,
                "column.include.list": format!("{}.id,{}.aggregate_id,{}.event_id,{}.event_type,{}.payload,{}.topic,{}.metadata,{}.partition_key,{}.schema_version,{}.created_at,{}.updated_at",
                    config.table_include_list, config.table_include_list, config.table_include_list, config.table_include_list,
                    config.table_include_list, config.table_include_list, config.table_include_list, config.table_include_list,
                    config.table_include_list, config.table_include_list, config.table_include_list),

                // Signal table for controlling connector
                "signal.data.collection": "public.debezium_signal",
                "signal.enabled.channels": "source,kafka",

                // Custom configuration for banking domain
                "topic.creation.default.replication.factor": 3,
                "topic.creation.default.partitions": 6,
                "topic.creation.default.cleanup.policy": "delete",
                "topic.creation.default.retention.ms": 604800000, // 7 days
                "topic.creation.default.segment.ms": 86400000, // 1 day
                "topic.creation.default.compression.type": "snappy"
            }
        })
    }

    /// Batch insert with optimized transaction handling
    pub async fn batch_insert_messages(&self, messages: Vec<CDCOutboxMessage>) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let _permit = self.write_semaphore.acquire().await?;
        let mut tx = self.pool.begin().await?;

        // Use COPY for better performance with large batches
        if messages.len() > 50 {
            let mut copy_writer = tx.copy_in_raw(
                "COPY kafka_outbox_cdc (aggregate_id, event_id, event_type, payload, topic, metadata, partition_key, schema_version, created_at, updated_at) FROM STDIN WITH (FORMAT CSV)"
            ).await?;

            for msg in messages {
                let partition_key = msg
                    .partition_key
                    .unwrap_or_else(|| msg.aggregate_id.to_string());
                let schema_version = msg.schema_version.unwrap_or(1);
                let metadata_str = msg
                    .metadata
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| "null".to_string());

                let row = format!(
                    "{},{},{},{},{},{},{},{},{}\n",
                    msg.aggregate_id,
                    msg.event_id,
                    msg.event_type,
                    base64::encode(&[]), // Placeholder for payload
                    msg.topic,
                    metadata_str,
                    partition_key,
                    schema_version,
                    msg.created_at.to_rfc3339(),
                    msg.updated_at.to_rfc3339()
                );
                copy_writer.send(row.as_bytes()).await?;
            }

            copy_writer.finish().await?;
        } else {
            // Use regular INSERT for smaller batches
            for msg in messages {
                let partition_key = msg
                    .partition_key
                    .unwrap_or_else(|| msg.aggregate_id.to_string());
                let schema_version = msg.schema_version.unwrap_or(1);

                sqlx::query!(
                    r#"
                    INSERT INTO kafka_outbox_cdc
                        (aggregate_id, event_id, event_type, payload, topic, metadata, partition_key, schema_version, created_at, updated_at)
                    VALUES
                        ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    "#,
                    msg.aggregate_id,
                    msg.event_id,
                    msg.event_type,
                    vec![0u8], // Placeholder payload
                    msg.topic,
                    msg.metadata,
                    partition_key,
                    schema_version,
                    msg.created_at,
                    msg.updated_at
                )
                .execute(&mut *tx)
                .await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }
}

/// Optimized CDC Event Processor with better performance and reliability
pub struct OptimizedCDCEventProcessor {
    kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
    cache_service: Arc<dyn CacheServiceTrait>,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    metrics: Arc<OptimizedCDCMetrics>,

    // Performance optimizations
    processing_semaphore: Arc<Semaphore>,
    batch_processor: Arc<RwLock<Vec<serde_json::Value>>>,
    batch_size: usize,
    batch_timeout: Duration,

    // Circuit breaker for reliability
    circuit_breaker: Arc<RwLock<CircuitBreakerState>>,

    // Projection cache to reduce database hits
    projection_cache: Arc<RwLock<HashMap<Uuid, CachedProjection>>>,
    cache_ttl: Duration,
}

#[derive(Debug, Default)]
pub struct OptimizedCDCMetrics {
    pub events_processed: std::sync::atomic::AtomicU64,
    pub events_failed: std::sync::atomic::AtomicU64,
    pub events_batched: std::sync::atomic::AtomicU64,
    pub processing_latency_ms: std::sync::atomic::AtomicU64,
    pub total_latency_ms: std::sync::atomic::AtomicU64,
    pub cache_invalidations: std::sync::atomic::AtomicU64,
    pub projection_updates: std::sync::atomic::AtomicU64,
    pub projection_cache_hits: std::sync::atomic::AtomicU64,
    pub projection_cache_misses: std::sync::atomic::AtomicU64,
    pub circuit_breaker_trips: std::sync::atomic::AtomicU64,
    pub batch_processing_time_ms: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone)]
struct CachedProjection {
    projection: crate::infrastructure::projections::AccountProjection,
    cached_at: Instant,
    version: u64,
}

#[derive(Debug, Clone)]
enum CircuitBreakerState {
    Closed,
    Open { until: Instant },
    HalfOpen,
}

impl Default for CircuitBreakerState {
    fn default() -> Self {
        CircuitBreakerState::Closed
    }
}

impl OptimizedCDCEventProcessor {
    pub fn new(
        kafka_producer: crate::infrastructure::kafka_abstraction::KafkaProducer,
        cache_service: Arc<dyn CacheServiceTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
    ) -> Self {
        Self {
            kafka_producer,
            cache_service,
            projection_store,
            metrics: Arc::new(OptimizedCDCMetrics::default()),
            processing_semaphore: Arc::new(Semaphore::new(50)), // Max 50 concurrent events
            batch_processor: Arc::new(RwLock::new(Vec::new())),
            batch_size: 10,
            batch_timeout: Duration::from_millis(100),
            circuit_breaker: Arc::new(RwLock::new(CircuitBreakerState::Closed)),
            projection_cache: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl: Duration::from_secs(300), // 5 minutes
        }
    }

    /// Process CDC event with optimized batching and caching
    pub async fn process_cdc_event_optimized(&self, cdc_event: serde_json::Value) -> Result<()> {
        let start_time = Instant::now();

        // Check circuit breaker
        if self.is_circuit_breaker_open().await {
            return Err(anyhow::anyhow!("Circuit breaker is open"));
        }

        let _permit = self.processing_semaphore.acquire().await?;

        // Extract outbox message
        let outbox_message_opt = self.extract_outbox_message_optimized(cdc_event)?;
        if outbox_message_opt.is_none() {
            return Ok(());
        }
        let outbox_message = outbox_message_opt.unwrap();

        // Deserialize domain event
        let domain_event: crate::domain::AccountEvent =
            bincode::deserialize(&outbox_message.payload[..])?;

        // Process with optimized projection updates
        match self
            .process_event_with_caching(&domain_event, outbox_message.aggregate_id)
            .await
        {
            Ok(_) => {
                self.reset_circuit_breaker().await;

                // Optimized cache invalidation with debouncing
                self.schedule_cache_invalidation(outbox_message.aggregate_id)
                    .await;

                let latency = start_time.elapsed().as_millis() as u64;
                self.metrics
                    .processing_latency_ms
                    .fetch_add(latency, std::sync::atomic::Ordering::Relaxed);
                self.metrics
                    .events_processed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                debug!(
                    "CDC event processed successfully: {} ({}ms)",
                    outbox_message.event_id, latency
                );
                Ok(())
            }
            Err(e) => {
                self.trip_circuit_breaker().await;
                self.metrics
                    .events_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Process event with projection caching
    async fn process_event_with_caching(
        &self,
        event: &crate::domain::AccountEvent,
        account_id: Uuid,
    ) -> Result<()> {
        let mut cache = self.projection_cache.write().await;
        let now = Instant::now();

        // Check cache first
        let cached_projection = cache.get(&account_id).cloned();
        drop(cache);

        let mut projection = if let Some(cached) = cached_projection {
            if now.duration_since(cached.cached_at) < self.cache_ttl {
                self.metrics
                    .projection_cache_hits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                cached.projection
            } else {
                self.metrics
                    .projection_cache_misses
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.load_projection_from_db(account_id).await?
            }
        } else {
            self.metrics
                .projection_cache_misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.load_projection_from_db(account_id).await?
        };

        // Apply event to projection
        self.apply_event_to_projection(&mut projection, event)?;

        // Update projection in database
        self.projection_store
            .upsert_accounts_batch(vec![projection.clone()])
            .await?;

        // Update cache
        let mut cache = self.projection_cache.write().await;
        cache.insert(
            account_id,
            CachedProjection {
                projection,
                cached_at: now,
                version: cache.get(&account_id).map(|c| c.version + 1).unwrap_or(1),
            },
        );

        self.metrics
            .projection_updates
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn load_projection_from_db(
        &self,
        account_id: Uuid,
    ) -> Result<crate::infrastructure::projections::AccountProjection> {
        if let Some(projection) = self.projection_store.get_account(account_id).await? {
            Ok(projection)
        } else {
            // Create new projection
            Ok(crate::infrastructure::projections::AccountProjection {
                id: account_id,
                owner_name: "".to_string(),
                balance: Decimal::ZERO,
                is_active: false,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
        }
    }

    fn apply_event_to_projection(
        &self,
        projection: &mut crate::infrastructure::projections::AccountProjection,
        event: &crate::domain::AccountEvent,
    ) -> Result<()> {
        match event {
            crate::domain::AccountEvent::MoneyDeposited { amount, .. } => {
                projection.balance += *amount;
                projection.updated_at = Utc::now();
            }
            crate::domain::AccountEvent::MoneyWithdrawn { amount, .. } => {
                projection.balance -= *amount;
                projection.updated_at = Utc::now();
            }
            crate::domain::AccountEvent::AccountCreated {
                owner_name,
                initial_balance,
                ..
            } => {
                projection.owner_name = owner_name.clone();
                projection.balance = *initial_balance;
                projection.is_active = true;
                projection.updated_at = Utc::now();
            }
            crate::domain::AccountEvent::AccountClosed { .. } => {
                projection.is_active = false;
                projection.updated_at = Utc::now();
            }
        }
        Ok(())
    }

    /// Optimized cache invalidation with debouncing
    async fn schedule_cache_invalidation(&self, account_id: Uuid) {
        let cache_service = self.cache_service.clone();
        let metrics = self.metrics.clone();

        // Use a small delay to batch cache invalidations
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Err(e) = cache_service.invalidate_account(account_id).await {
                error!(
                    "Failed to invalidate cache for account {}: {}",
                    account_id, e
                );
            } else {
                metrics
                    .cache_invalidations
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });
    }

    /// Enhanced message extraction with better error handling
    fn extract_outbox_message_optimized(
        &self,
        cdc_event: serde_json::Value,
    ) -> Result<Option<CDCOutboxMessageWithPayload>> {
        let payload = cdc_event
            .get("payload")
            .ok_or_else(|| anyhow::anyhow!("CDC event missing payload field"))?;

        let after = payload.get("after");
        if after.is_none() || after == Some(&serde_json::Value::Null) {
            debug!("CDC event is a delete/tombstone, skipping");
            return Ok(None);
        }

        let after_data = after.unwrap();
        let payload_str = after_data
            .get("payload")
            .and_then(|p| p.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing payload field in CDC event"))?;

        let payload_bytes = base64::decode(payload_str)
            .map_err(|e| anyhow::anyhow!("Failed to decode base64 payload: {}", e))?;

        let outbox_message: CDCOutboxMessage = serde_json::from_value(after_data.clone())
            .map_err(|e| anyhow::anyhow!("Failed to deserialize CDC message: {}", e))?;

        Ok(Some(CDCOutboxMessageWithPayload {
            id: outbox_message.id,
            aggregate_id: outbox_message.aggregate_id,
            event_id: outbox_message.event_id,
            event_type: outbox_message.event_type,
            payload: payload_bytes,
            topic: outbox_message.topic,
            metadata: outbox_message.metadata,
            created_at: outbox_message.created_at,
            updated_at: outbox_message.updated_at,
        }))
    }

    /// Circuit breaker implementation
    async fn is_circuit_breaker_open(&self) -> bool {
        let state = self.circuit_breaker.read().await;
        match *state {
            CircuitBreakerState::Open { until } => Instant::now() < until,
            _ => false,
        }
    }

    async fn trip_circuit_breaker(&self) {
        let mut state = self.circuit_breaker.write().await;
        *state = CircuitBreakerState::Open {
            until: Instant::now() + Duration::from_secs(30),
        };
        self.metrics
            .circuit_breaker_trips
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        warn!("Circuit breaker tripped for CDC event processor");
    }

    async fn reset_circuit_breaker(&self) {
        let mut state = self.circuit_breaker.write().await;
        if matches!(*state, CircuitBreakerState::Open { .. }) {
            *state = CircuitBreakerState::Closed;
            info!("Circuit breaker reset for CDC event processor");
        }
    }

    pub fn get_metrics(&self) -> &OptimizedCDCMetrics {
        &self.metrics
    }
}

#[derive(Debug, Clone)]
pub struct CDCOutboxMessageWithPayload {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub event_id: Uuid,
    pub event_type: String,
    pub payload: Vec<u8>,
    pub topic: String,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Implementation of the existing OutboxRepositoryTrait for CDC with optimizations
#[async_trait]
impl crate::infrastructure::outbox::OutboxRepositoryTrait for OptimizedCDCOutboxRepository {
    async fn add_pending_messages(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        messages: Vec<crate::infrastructure::outbox::OutboxMessage>,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        // Convert to CDC messages with sequence numbers
        let mut cdc_messages = Vec::with_capacity(messages.len());
        for msg in messages {
            let sequence = self.get_next_sequence().await?;
            cdc_messages.push(CDCOutboxMessage {
                id: Uuid::new_v4(),
                aggregate_id: msg.aggregate_id,
                event_id: msg.event_id,
                event_type: msg.event_type,
                topic: msg.topic,
                metadata: msg.metadata,
                sequence_number: sequence,
                correlation_id: None, // Could be derived from metadata
                created_at: Utc::now(),
                updated_at: Utc::now(),
            });
        }

        // Use batch insert for better performance
        self.batch_insert_outbox_messages(tx, cdc_messages).await?;

        Ok(())
    }

    // Keep existing interface methods as no-ops for CDC
    async fn fetch_and_lock_pending_messages(
        &self,
        _limit: i64,
    ) -> Result<Vec<crate::infrastructure::outbox::PersistedOutboxMessage>> {
        Ok(Vec::new())
    }

    async fn mark_as_processed(&self, _outbox_message_id: Uuid) -> Result<()> {
        Ok(())
    }

    async fn delete_processed_batch(&self, _outbox_message_ids: &[Uuid]) -> Result<usize> {
        Ok(0)
    }

    async fn record_failed_attempt(
        &self,
        _outbox_message_id: Uuid,
        _max_retries: i32,
        _error_message: Option<String>,
    ) -> Result<()> {
        Ok(())
    }

    async fn mark_as_failed(
        &self,
        _outbox_message_id: Uuid,
        _error_message: Option<String>,
    ) -> Result<()> {
        Ok(())
    }

    async fn find_stuck_processing_messages(
        &self,
        _stuck_threshold: Duration,
        _limit: i32,
    ) -> Result<Vec<crate::infrastructure::outbox::PersistedOutboxMessage>> {
        Ok(Vec::new())
    }

    async fn reset_stuck_messages(&self, _outbox_message_ids: &[Uuid]) -> Result<usize> {
        Ok(0)
    }
}
