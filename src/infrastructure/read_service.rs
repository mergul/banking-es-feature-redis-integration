// src/infrastructure/direct_read_service.rs

use crate::domain::{Account, AccountEvent};
use crate::infrastructure::projections::{AccountProjection, ProjectionStoreTrait};
use anyhow::Result;
use futures::future::join_all;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Simplified read operation types
#[derive(Debug, Clone)]
pub enum ReadOperation {
    GetAccount { account_id: Uuid },
    GetAccountTransactions { account_id: Uuid },
    GetMultipleAccounts { account_ids: Vec<Uuid> },
    GetAccountBalance { account_id: Uuid },
}

/// Simplified result type
#[derive(Debug, Clone)]
pub enum ReadOperationResult {
    Account {
        account_id: Uuid,
        account: Option<AccountProjection>,
    },
    AccountTransactions {
        account_id: Uuid,
        transactions: Vec<AccountEvent>,
    },
    MultipleAccounts {
        accounts: Vec<AccountProjection>,
    },
    AccountBalance {
        account_id: Uuid,
        balance: Option<Decimal>,
    },
}

/// Ultra-fast direct read service without batching overhead
pub struct DirectReadService {
    // Multiple read pools for parallel processing
    read_pools: Vec<Arc<PgPool>>,
    projection_store: Arc<dyn ProjectionStoreTrait>,

    // Simple in-memory cache with RwLock for better read performance
    account_cache: Arc<RwLock<HashMap<Uuid, (AccountProjection, Instant)>>>,
    cache_ttl_secs: u64,
}

impl DirectReadService {
    pub fn new(
        read_pools: Vec<Arc<PgPool>>,
        projection_store: Arc<dyn ProjectionStoreTrait>,
        cache_ttl_secs: u64,
    ) -> Self {
        info!(
            "🚀 Initializing DirectReadService with {} read pools",
            read_pools.len()
        );
        Self {
            read_pools,
            projection_store,
            account_cache: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl_secs,
        }
    }

    /// Process multiple read operations in true parallel without batching overhead
    pub async fn process_read_operations_parallel(
        &self,
        operations: Vec<ReadOperation>,
    ) -> Result<Vec<ReadOperationResult>> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        let start = Instant::now();
        info!(
            "🚀 Processing {} read operations in parallel",
            operations.len()
        );

        // Group operations by type for optimal processing
        let mut account_reads = Vec::new();
        let mut transaction_reads = Vec::new();
        let mut multiple_account_reads = Vec::new();
        let mut balance_reads = Vec::new();

        for (idx, op) in operations.into_iter().enumerate() {
            match op {
                ReadOperation::GetAccount { account_id } => {
                    account_reads.push((idx, account_id));
                }
                ReadOperation::GetAccountTransactions { account_id } => {
                    transaction_reads.push((idx, account_id));
                }
                ReadOperation::GetMultipleAccounts { account_ids } => {
                    multiple_account_reads.push((idx, account_ids));
                }
                ReadOperation::GetAccountBalance { account_id } => {
                    balance_reads.push((idx, account_id));
                }
            }
        }

        // Process all operation types in parallel
        let (account_results, transaction_results, multiple_results, balance_results) = tokio::join!(
            self.process_account_reads_parallel(account_reads),
            self.process_transaction_reads_parallel(transaction_reads),
            self.process_multiple_account_reads_parallel(multiple_account_reads),
            self.process_balance_reads_parallel(balance_reads)
        );

        // Combine results in original order
        let mut combined_results = Vec::new();
        combined_results.extend(account_results);
        combined_results.extend(transaction_results);
        combined_results.extend(multiple_results);
        combined_results.extend(balance_results);

        // Sort by original index to maintain order
        combined_results.sort_by_key(|(idx, _)| *idx);
        let final_results: Vec<ReadOperationResult> = combined_results
            .into_iter()
            .map(|(_, result)| result)
            .collect();

        let duration = start.elapsed();
        let ops_per_sec = final_results.len() as f64 / duration.as_secs_f64();
        info!(
            "✅ Completed {} parallel reads in {:?} ({:.2} ops/sec)",
            final_results.len(),
            duration,
            ops_per_sec
        );

        Ok(final_results)
    }

    /// Process account reads with intelligent caching and pool distribution
    async fn process_account_reads_parallel(
        &self,
        account_reads: Vec<(usize, Uuid)>,
    ) -> Vec<(usize, ReadOperationResult)> {
        if account_reads.is_empty() {
            return Vec::new();
        }

        // Check cache first
        let mut cache_hits = Vec::new();
        let mut cache_misses = Vec::new();
        let cache_cutoff = Instant::now() - std::time::Duration::from_secs(self.cache_ttl_secs);

        {
            let cache = self.account_cache.read().await;
            for (idx, account_id) in account_reads {
                if let Some((account, timestamp)) = cache.get(&account_id) {
                    if timestamp > &cache_cutoff {
                        cache_hits.push((
                            idx,
                            ReadOperationResult::Account {
                                account_id,
                                account: Some(account.clone()),
                            },
                        ));
                        continue;
                    }
                }
                cache_misses.push((idx, account_id));
            }
        }

        info!(
            "📊 Cache stats: {} hits, {} misses",
            cache_hits.len(),
            cache_misses.len()
        );

        // Process cache misses in parallel across multiple pools
        let mut database_results = Vec::new();
        if !cache_misses.is_empty() {
            let chunk_size =
                (cache_misses.len() + self.read_pools.len() - 1) / self.read_pools.len();
            let chunks: Vec<_> = cache_misses.chunks(chunk_size).enumerate().collect();

            let chunk_futures = chunks.into_iter().map(|(pool_idx, chunk)| {
                let pool = self.read_pools[pool_idx % self.read_pools.len()].clone();
                let account_cache = self.account_cache.clone();
                let chunk = chunk.to_vec();

                async move {
                    let mut results = Vec::new();
                    let account_ids: Vec<Uuid> = chunk.iter().map(|(_, id)| *id).collect();

                    // Batch query for this chunk
                    match Self::batch_get_accounts(&pool, &account_ids).await {
                        Ok(accounts) => {
                            let accounts_map: HashMap<Uuid, AccountProjection> =
                                accounts.into_iter().map(|acc| (acc.id, acc)).collect();

                            // Update cache and prepare results
                            {
                                let mut cache = account_cache.write().await;
                                for (idx, account_id) in chunk {
                                    let account = accounts_map.get(&account_id).cloned();
                                    if let Some(ref acc) = account {
                                        cache.insert(account_id, (acc.clone(), Instant::now()));
                                    }
                                    results.push((
                                        idx,
                                        ReadOperationResult::Account {
                                            account_id,
                                            account,
                                        },
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            error!("Database query failed for chunk: {}", e);
                            // Return None results on error
                            for (idx, account_id) in chunk {
                                results.push((
                                    idx,
                                    ReadOperationResult::Account {
                                        account_id,
                                        account: None,
                                    },
                                ));
                            }
                        }
                    }
                    results
                }
            });

            let chunk_results = join_all(chunk_futures).await;
            for chunk_result in chunk_results {
                database_results.extend(chunk_result);
            }
        }

        // Combine cache hits and database results
        let mut all_results = cache_hits;
        all_results.extend(database_results);
        all_results
    }

    /// Process transaction reads in parallel
    async fn process_transaction_reads_parallel(
        &self,
        transaction_reads: Vec<(usize, Uuid)>,
    ) -> Vec<(usize, ReadOperationResult)> {
        if transaction_reads.is_empty() {
            return Vec::new();
        }

        let chunk_size =
            (transaction_reads.len() + self.read_pools.len() - 1) / self.read_pools.len();
        let chunks: Vec<_> = transaction_reads.chunks(chunk_size).enumerate().collect();

        let chunk_futures = chunks.into_iter().map(|(pool_idx, chunk)| {
            let projection_store = self.projection_store.clone();
            let chunk = chunk.to_vec();

            async move {
                let mut results = Vec::new();
                for (idx, account_id) in chunk {
                    let transactions = projection_store
                        .get_account_transactions(account_id)
                        .await
                        .unwrap_or_default();

                    // Convert to AccountEvent (simplified)
                    let account_events: Vec<AccountEvent> = transactions
                        .into_iter()
                        .map(|tx| AccountEvent::MoneyDeposited {
                            account_id: tx.account_id,
                            amount: tx.amount,
                            transaction_id: tx.id,
                        })
                        .collect();

                    results.push((
                        idx,
                        ReadOperationResult::AccountTransactions {
                            account_id,
                            transactions: account_events,
                        },
                    ));
                }
                results
            }
        });

        let chunk_results = join_all(chunk_futures).await;
        let mut all_results = Vec::new();
        for chunk_result in chunk_results {
            all_results.extend(chunk_result);
        }
        all_results
    }

    /// Process multiple account reads efficiently
    async fn process_multiple_account_reads_parallel(
        &self,
        multiple_reads: Vec<(usize, Vec<Uuid>)>,
    ) -> Vec<(usize, ReadOperationResult)> {
        if multiple_reads.is_empty() {
            return Vec::new();
        }

        let futures = multiple_reads
            .into_iter()
            .enumerate()
            .map(|(_, (idx, account_ids))| {
                let pool_idx = idx % self.read_pools.len();
                let pool = self.read_pools[pool_idx].clone();

                async move {
                    let accounts = Self::batch_get_accounts(&pool, &account_ids)
                        .await
                        .unwrap_or_default();
                    (idx, ReadOperationResult::MultipleAccounts { accounts })
                }
            });

        join_all(futures).await
    }

    /// Process balance reads in parallel
    async fn process_balance_reads_parallel(
        &self,
        balance_reads: Vec<(usize, Uuid)>,
    ) -> Vec<(usize, ReadOperationResult)> {
        if balance_reads.is_empty() {
            return Vec::new();
        }

        let chunk_size = (balance_reads.len() + self.read_pools.len() - 1) / self.read_pools.len();
        let chunks: Vec<_> = balance_reads.chunks(chunk_size).enumerate().collect();

        let chunk_futures = chunks.into_iter().map(|(pool_idx, chunk)| {
            let pool = self.read_pools[pool_idx % self.read_pools.len()].clone();
            let chunk = chunk.to_vec();

            async move {
                let mut results = Vec::new();
                for (idx, account_id) in chunk {
                    let balance = Self::get_account_balance_direct(&pool, account_id).await;
                    results.push((
                        idx,
                        ReadOperationResult::AccountBalance {
                            account_id,
                            balance,
                        },
                    ));
                }
                results
            }
        });

        let chunk_results = join_all(chunk_futures).await;
        let mut all_results = Vec::new();
        for chunk_result in chunk_results {
            all_results.extend(chunk_result);
        }
        all_results
    }

    /// Direct database queries without overhead
    async fn batch_get_accounts(
        pool: &PgPool,
        account_ids: &[Uuid],
    ) -> Result<Vec<AccountProjection>> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as!(
            AccountProjection,
            "SELECT id, owner_name, balance, is_active, created_at, updated_at 
             FROM account_projections 
             WHERE id = ANY($1)
             ORDER BY id DESC",
            account_ids
        )
        .fetch_all(pool)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get accounts: {}", e))
    }

    async fn get_account_balance_direct(pool: &PgPool, account_id: Uuid) -> Option<Decimal> {
        sqlx::query!(
            "SELECT balance FROM account_projections WHERE id = $1",
            account_id
        )
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .map(|row| row.balance)
    }

    /// Simple cache cleanup
    pub async fn cleanup_cache(&self) {
        let cache_cutoff = Instant::now() - std::time::Duration::from_secs(self.cache_ttl_secs);
        let mut cache = self.account_cache.write().await;
        let before_count = cache.len();
        cache.retain(|_, (_, timestamp)| *timestamp > cache_cutoff);
        let after_count = cache.len();
        if before_count != after_count {
            info!(
                "🧹 Cache cleanup: removed {} expired entries",
                before_count - after_count
            );
        }
    }

    /// Get cache statistics
    pub async fn get_cache_stats(&self) -> (usize, f64) {
        let cache = self.account_cache.read().await;
        let total_entries = cache.len();
        let cache_cutoff = Instant::now() - std::time::Duration::from_secs(self.cache_ttl_secs);
        let valid_entries = cache
            .values()
            .filter(|(_, timestamp)| timestamp > &cache_cutoff)
            .count();
        let hit_rate = if total_entries > 0 {
            (valid_entries as f64 / total_entries as f64) * 100.0
        } else {
            0.0
        };
        (valid_entries, hit_rate)
    }
}

/// Integration helper for existing codebase
impl DirectReadService {
    pub async fn process_single_operation(
        &self,
        operation: ReadOperation,
    ) -> Result<ReadOperationResult> {
        let results = self
            .process_read_operations_parallel(vec![operation])
            .await?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No result returned"))
    }

    pub async fn get_account(&self, account_id: Uuid) -> Result<Option<AccountProjection>> {
        match self
            .process_single_operation(ReadOperation::GetAccount { account_id })
            .await?
        {
            ReadOperationResult::Account { account, .. } => Ok(account),
            _ => Err(anyhow::anyhow!("Unexpected result type")),
        }
    }

    pub async fn get_account_balance(&self, account_id: Uuid) -> Result<Option<Decimal>> {
        match self
            .process_single_operation(ReadOperation::GetAccountBalance { account_id })
            .await?
        {
            ReadOperationResult::AccountBalance { balance, .. } => Ok(balance),
            _ => Err(anyhow::anyhow!("Unexpected result type")),
        }
    }

    pub async fn get_multiple_accounts(
        &self,
        account_ids: Vec<Uuid>,
    ) -> Result<Vec<AccountProjection>> {
        match self
            .process_single_operation(ReadOperation::GetMultipleAccounts { account_ids })
            .await?
        {
            ReadOperationResult::MultipleAccounts { accounts } => Ok(accounts),
            _ => Err(anyhow::anyhow!("Unexpected result type")),
        }
    }
}
