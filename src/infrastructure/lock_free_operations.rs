use crate::domain::{Account, AccountEvent};
use crate::infrastructure::event_store::EventStoreTrait;
use crate::infrastructure::projections::{AccountProjection, ProjectionStoreTrait};
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Configuration for lock-free operations
#[derive(Debug, Clone)]
pub struct LockFreeConfig {
    pub enable_read_caching: bool,
    pub cache_ttl_secs: u64,
    pub max_cache_size: usize,
    pub enable_optimistic_reads: bool,
    pub enable_eventual_consistency: bool,
    pub consistency_timeout_ms: u64,
    pub batch_read_size: usize,
    pub enable_parallel_reads: bool,
    pub max_parallel_reads: usize,
}

impl Default for LockFreeConfig {
    fn default() -> Self {
        Self {
            enable_read_caching: true,
            cache_ttl_secs: 300, // 5 minutes
            max_cache_size: 10000,
            enable_optimistic_reads: true,
            enable_eventual_consistency: true,
            consistency_timeout_ms: 1000, // 1 second
            batch_read_size: 100,
            enable_parallel_reads: true,
            max_parallel_reads: 10,
        }
    }
}

/// Metrics for lock-free operations
#[derive(Debug)]
pub struct LockFreeMetrics {
    pub read_operations: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub optimistic_reads: AtomicU64,
    pub consistency_checks: AtomicU64,
    pub parallel_reads: AtomicU64,
    pub avg_read_time_us: AtomicU64,
    pub total_read_time_us: AtomicU64,
}

impl Default for LockFreeMetrics {
    fn default() -> Self {
        Self {
            read_operations: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            optimistic_reads: AtomicU64::new(0),
            consistency_checks: AtomicU64::new(0),
            parallel_reads: AtomicU64::new(0),
            avg_read_time_us: AtomicU64::new(0),
            total_read_time_us: AtomicU64::new(0),
        }
    }
}

/// Cache entry for lock-free reads
#[derive(Debug, Clone)]
struct CacheEntry<T> {
    value: T,
    created_at: Instant,
    version: i64,
    ttl: Duration,
}

impl<T> CacheEntry<T> {
    fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.ttl
    }
}

/// Lock-free operations service for read-only operations
pub struct LockFreeOperations {
    event_store: Arc<dyn EventStoreTrait>,
    projection_store: Arc<dyn ProjectionStoreTrait>,
    config: LockFreeConfig,
    metrics: Arc<LockFreeMetrics>,
    account_cache: Arc<DashMap<Uuid, CacheEntry<Account>>>,
    projection_cache: Arc<DashMap<Uuid, CacheEntry<AccountProjection>>>,
    event_cache: Arc<DashMap<Uuid, CacheEntry<Vec<AccountEvent>>>>,
    version_cache: Arc<DashMap<Uuid, i64>>,
}

#[async_trait]
pub trait LockFreeOperationsTrait: Send + Sync {
    async fn get_account(&self, account_id: Uuid) -> Result<Option<Account>>;
    async fn get_account_projection(&self, account_id: Uuid) -> Result<Option<AccountProjection>>;
    async fn get_account_events(
        &self,
        account_id: Uuid,
        from_version: Option<i64>,
    ) -> Result<Vec<AccountEvent>>;
    async fn get_multiple_accounts(&self, account_ids: Vec<Uuid>) -> Result<Vec<Account>>;
    async fn get_all_accounts(&self) -> Result<Vec<Account>>;
    async fn get_accounts_by_owner(&self, owner_name: &str) -> Result<Vec<Account>>;
    async fn get_metrics(&self) -> &LockFreeMetrics;
    async fn get_metrics_json(&self) -> serde_json::Value;
    async fn clear_cache(&self) -> Result<()>;
    async fn health_check(&self) -> Result<bool>;
}

impl LockFreeOperations {
    pub fn new(
        event_store: Arc<dyn EventStoreTrait>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        config: LockFreeConfig,
    ) -> Self {
        Self {
            event_store,
            projection_store,
            config,
            metrics: Arc::new(LockFreeMetrics::default()),
            account_cache: Arc::new(DashMap::new()),
            projection_cache: Arc::new(DashMap::new()),
            event_cache: Arc::new(DashMap::new()),
            version_cache: Arc::new(DashMap::new()),
        }
    }

    /// Cleanup expired cache entries
    async fn cleanup_expired_entries(&self) {
        let now = Instant::now();
        let ttl = Duration::from_secs(self.config.cache_ttl_secs);

        // Cleanup account cache
        self.account_cache.retain(|_, entry| !entry.is_expired());

        // Cleanup projection cache
        self.projection_cache.retain(|_, entry| !entry.is_expired());

        // Cleanup event cache
        self.event_cache.retain(|_, entry| !entry.is_expired());

        debug!("Cache cleanup completed");
    }

    /// Update metrics with read time
    fn update_read_metrics(&self, duration: Duration) {
        let duration_us = duration.as_micros() as u64;
        let total_time = self
            .metrics
            .total_read_time_us
            .fetch_add(duration_us, Ordering::Relaxed)
            + duration_us;
        let total_ops = self.metrics.read_operations.fetch_add(1, Ordering::Relaxed) + 1;

        // Update average read time
        let avg_time = total_time / total_ops;
        self.metrics
            .avg_read_time_us
            .store(avg_time, Ordering::Relaxed);
    }

    /// Check if data is consistent (optimistic consistency check)
    async fn check_consistency(&self, account_id: Uuid, expected_version: i64) -> bool {
        if !self.config.enable_optimistic_reads {
            return true;
        }

        self.metrics
            .consistency_checks
            .fetch_add(1, Ordering::Relaxed);

        match self
            .event_store
            .get_current_version(account_id, false)
            .await
        {
            Ok(current_version) => {
                let is_consistent = current_version >= expected_version;
                if !is_consistent {
                    warn!(
                        "Inconsistent data detected for account {}: expected {}, got {}",
                        account_id, expected_version, current_version
                    );
                }
                is_consistent
            }
            Err(_) => {
                warn!("Failed to check consistency for account {}", account_id);
                false
            }
        }
    }

    /// Perform optimistic read with consistency check
    async fn optimistic_read<F, Fut, T>(&self, account_id: Uuid, operation: F) -> Result<Option<T>>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<Option<T>>> + Send,
        T: Clone + Send + 'static,
    {
        self.metrics
            .optimistic_reads
            .fetch_add(1, Ordering::Relaxed);

        // Get current version for consistency check
        let current_version = self
            .event_store
            .get_current_version(account_id, false)
            .await?;

        // Perform the read operation
        let result = operation().await?;

        // Check consistency if data was found
        if result.is_some() {
            let is_consistent = self.check_consistency(account_id, current_version).await;
            if !is_consistent && self.config.enable_eventual_consistency {
                // For eventual consistency, we can still return the result
                // but log the inconsistency
                debug!(
                    "Returning potentially stale data for account {} (eventual consistency)",
                    account_id
                );
            } else if !is_consistent {
                // For strict consistency, return None if inconsistent
                return Ok(None);
            }
        }

        Ok(result)
    }
}

#[async_trait]
impl LockFreeOperationsTrait for LockFreeOperations {
    async fn get_account(&self, account_id: Uuid) -> Result<Option<Account>> {
        let start = Instant::now();
        self.metrics.read_operations.fetch_add(1, Ordering::Relaxed);

        // Try cache first if enabled
        if self.config.enable_read_caching {
            if let Some(entry) = self.account_cache.get(&account_id) {
                if !entry.is_expired() {
                    self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                    self.update_read_metrics(start.elapsed());
                    return Ok(Some(entry.value.clone()));
                }
            }
        }

        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Perform optimistic read from event store
        let result: Option<Account> = self
            .optimistic_read(account_id, || async {
                self.event_store
                    .get_account(account_id)
                    .await
                    .map_err(|e| anyhow::anyhow!("Event store error: {}", e))
            })
            .await?;

        // Cache the result if found
        if let Some(account) = &result {
            if self.config.enable_read_caching {
                let entry = CacheEntry {
                    value: account.clone(),
                    created_at: Instant::now(),
                    version: account.version,
                    ttl: Duration::from_secs(self.config.cache_ttl_secs),
                };
                self.account_cache.insert(account_id, entry);
            }
        }

        self.update_read_metrics(start.elapsed());
        Ok(result)
    }

    async fn get_account_projection(&self, account_id: Uuid) -> Result<Option<AccountProjection>> {
        let start = Instant::now();
        self.metrics.read_operations.fetch_add(1, Ordering::Relaxed);

        // Try cache first if enabled
        if self.config.enable_read_caching {
            if let Some(entry) = self.projection_cache.get(&account_id) {
                if !entry.is_expired() {
                    self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                    self.update_read_metrics(start.elapsed());
                    return Ok(Some(entry.value.clone()));
                }
            }
        }

        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Perform optimistic read from projection store
        let result = self
            .optimistic_read(account_id, || async {
                self.projection_store.get_account(account_id).await
            })
            .await?;

        // Cache the result if found
        if let Some(projection) = &result {
            if self.config.enable_read_caching {
                let entry = CacheEntry {
                    value: projection.clone(),
                    created_at: Instant::now(),
                    version: 0, // AccountProjection doesn't have version field
                    ttl: Duration::from_secs(self.config.cache_ttl_secs),
                };
                self.projection_cache.insert(account_id, entry);
            }
        }

        self.update_read_metrics(start.elapsed());
        Ok(result)
    }

    async fn get_account_events(
        &self,
        account_id: Uuid,
        from_version: Option<i64>,
    ) -> Result<Vec<AccountEvent>> {
        let start = Instant::now();
        self.metrics.read_operations.fetch_add(1, Ordering::Relaxed);

        // Try cache first if enabled and no from_version specified
        if self.config.enable_read_caching && from_version.is_none() {
            if let Some(entry) = self.event_cache.get(&account_id) {
                if !entry.is_expired() {
                    self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                    self.update_read_metrics(start.elapsed());
                    return Ok(entry.value.clone());
                }
            }
        }

        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Perform optimistic read from event store
        let result = self
            .optimistic_read(account_id, || async {
                let events = self
                    .event_store
                    .get_events(account_id, from_version)
                    .await?;
                // Convert Event to AccountEvent
                let account_events: Vec<AccountEvent> = events
                    .into_iter()
                    .filter_map(|event| {
                        if event.event_type == "AccountEvent" {
                            bincode::deserialize(&event.event_data).ok()
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok(Some(account_events))
            })
            .await?;

        let events = result.unwrap_or_default();

        // Cache the result if no from_version specified
        if self.config.enable_read_caching && from_version.is_none() {
            let entry = CacheEntry {
                value: events.clone(),
                created_at: Instant::now(),
                version: events.len() as i64,
                ttl: Duration::from_secs(self.config.cache_ttl_secs),
            };
            self.event_cache.insert(account_id, entry);
        }

        self.update_read_metrics(start.elapsed());
        Ok(events)
    }

    async fn get_multiple_accounts(&self, account_ids: Vec<Uuid>) -> Result<Vec<Account>> {
        let start = Instant::now();
        self.metrics.read_operations.fetch_add(1, Ordering::Relaxed);

        if self.config.enable_parallel_reads && account_ids.len() > 1 {
            self.metrics.parallel_reads.fetch_add(1, Ordering::Relaxed);

            // Use parallel reads for better performance
            let mut futures = Vec::new();
            for account_id in account_ids {
                let future = self.get_account(account_id);
                futures.push(future);
            }

            let results = futures::future::join_all(futures).await;
            let mut accounts = Vec::new();

            for result in results {
                if let Ok(Some(account)) = result {
                    accounts.push(account);
                }
            }

            self.update_read_metrics(start.elapsed());
            Ok(accounts)
        } else {
            // Sequential reads
            let mut accounts = Vec::new();
            for account_id in account_ids {
                if let Ok(Some(account)) = self.get_account(account_id).await {
                    accounts.push(account);
                }
            }

            self.update_read_metrics(start.elapsed());
            Ok(accounts)
        }
    }

    async fn get_all_accounts(&self) -> Result<Vec<Account>> {
        let start = Instant::now();
        self.metrics.read_operations.fetch_add(1, Ordering::Relaxed);

        let result = self.event_store.get_all_accounts().await?;
        self.update_read_metrics(start.elapsed());
        Ok(result)
    }

    async fn get_accounts_by_owner(&self, owner_name: &str) -> Result<Vec<Account>> {
        let start = Instant::now();
        self.metrics.read_operations.fetch_add(1, Ordering::Relaxed);

        // This would need to be implemented in the event store or projection store
        // For now, get all accounts and filter
        let all_accounts = self.get_all_accounts().await?;
        let filtered_accounts: Vec<Account> = all_accounts
            .into_iter()
            .filter(|account| account.owner_name == owner_name)
            .collect();

        self.update_read_metrics(start.elapsed());
        Ok(filtered_accounts)
    }

    async fn get_metrics(&self) -> &LockFreeMetrics {
        &self.metrics
    }

    async fn get_metrics_json(&self) -> serde_json::Value {
        let total_ops = self.metrics.read_operations.load(Ordering::Relaxed);
        let cache_hits = self.metrics.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.metrics.cache_misses.load(Ordering::Relaxed);
        let cache_hit_rate = if total_ops > 0 {
            (cache_hits as f64 / total_ops as f64) * 100.0
        } else {
            0.0
        };

        serde_json::json!({
            "read_operations": total_ops,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "cache_hit_rate": cache_hit_rate,
            "optimistic_reads": self.metrics.optimistic_reads.load(Ordering::Relaxed),
            "consistency_checks": self.metrics.consistency_checks.load(Ordering::Relaxed),
            "parallel_reads": self.metrics.parallel_reads.load(Ordering::Relaxed),
            "avg_read_time_us": self.metrics.avg_read_time_us.load(Ordering::Relaxed),
            "total_read_time_us": self.metrics.total_read_time_us.load(Ordering::Relaxed),
            "account_cache_size": self.account_cache.len(),
            "projection_cache_size": self.projection_cache.len(),
            "event_cache_size": self.event_cache.len(),
        })
    }

    async fn clear_cache(&self) -> Result<()> {
        self.account_cache.clear();
        self.projection_cache.clear();
        self.event_cache.clear();
        self.version_cache.clear();
        info!("Lock-free operations cache cleared");
        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        // Simple health check - try to get a non-existent account
        let test_id = Uuid::new_v4();
        match self.get_account(test_id).await {
            Ok(_) => Ok(true), // Should return None, but no error
            Err(_) => Ok(false),
        }
    }
}

#[cfg(test)]
#[ignore]
mod tests {
    use super::*;
    use crate::infrastructure::event_store::EventStore;
    use crate::infrastructure::projections::{ProjectionConfig, ProjectionStore};
    use sqlx::PgPool;

    #[tokio::test]
    #[ignore]
    async fn test_lock_free_operations_creation() {
        let pool = PgPool::connect("postgresql://localhost/test")
            .await
            .unwrap();
        let event_store = Arc::new(EventStore::new(pool.clone()));
        let projection_config = ProjectionConfig::default();
        let projection_store = Arc::new(ProjectionStore::new(projection_config).await.unwrap());
        let config = LockFreeConfig::default();

        let lock_free_ops = LockFreeOperations::new(event_store, projection_store, config);
        assert!(lock_free_ops.health_check().await.unwrap());
    }

    #[tokio::test]
    #[ignore]
    async fn test_cache_operations() {
        let pool = PgPool::connect("postgresql://localhost/test")
            .await
            .unwrap();
        let event_store = Arc::new(EventStore::new(pool.clone()));
        let projection_config = ProjectionConfig::default();
        let projection_store = Arc::new(ProjectionStore::new(projection_config).await.unwrap());
        let config = LockFreeConfig::default();

        let lock_free_ops = LockFreeOperations::new(event_store, projection_store, config);

        // Test cache clearing
        assert!(lock_free_ops.clear_cache().await.is_ok());
    }
}
