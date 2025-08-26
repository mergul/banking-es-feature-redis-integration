use crate::infrastructure::projections::{AccountProjection, ProjectionStoreTrait};
use crate::infrastructure::TransactionProjection;
use anyhow::Result;
use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

// OPTIMIZATION 8: Type-specific processors
#[derive(Clone)]
pub struct OptimizedCDCProcessor {
    projection_store: Arc<dyn ProjectionStoreTrait>,
    // Metrics
    metrics: ProcessorMetrics,
}

#[derive(Clone)]
pub struct ProcessorMetrics {
    pub accounts_created: Arc<AtomicUsize>,
    pub accounts_updated: Arc<AtomicUsize>,
    pub transactions_created: Arc<AtomicUsize>,
    pub batches_processed: Arc<AtomicUsize>,
}

impl ProcessorMetrics {
    pub fn new() -> Self {
        Self {
            accounts_created: Arc::new(AtomicUsize::new(0)),
            accounts_updated: Arc::new(AtomicUsize::new(0)),
            transactions_created: Arc::new(AtomicUsize::new(0)),
            batches_processed: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_totals(&self) -> (usize, usize, usize, usize) {
        (
            self.accounts_created.load(Ordering::Relaxed),
            self.accounts_updated.load(Ordering::Relaxed),
            self.transactions_created.load(Ordering::Relaxed),
            self.batches_processed.load(Ordering::Relaxed),
        )
    }
}

impl OptimizedCDCProcessor {
    pub async fn new(projection_store: Arc<dyn ProjectionStoreTrait>) -> Self {
        tracing::info!("Creating OptimizedCDCProcessor");
        Self {
            projection_store,
            metrics: ProcessorMetrics::new(),
        }
    }

    // NEW: Bulk account creation submission - OPTIMIZED
    pub async fn submit_account_creations_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>,
    ) -> Result<Vec<Uuid>> {
        tracing::debug!("Submitting account creations in bulk");
        if projections.is_empty() {
            return Ok(vec![]);
        }
        let operation_ids = projections.iter().map(|(id, _)| *id).collect();
        let account_projections: Vec<AccountProjection> =
            projections.into_iter().map(|(_, p)| p).collect();

        self.projection_store
            .bulk_insert_new_accounts_with_copy(account_projections)
            .await?;

        Ok(operation_ids)
    }

    // NEW: Bulk account update submission
    pub async fn submit_account_updates_bulk(
        &self,
        projections: Vec<(Uuid, AccountProjection)>,
    ) -> Result<Vec<Uuid>> {
        tracing::debug!("Submitting account updates in bulk");
        if projections.is_empty() {
            return Ok(vec![]);
        }
        let operation_ids = projections.iter().map(|(id, _)| *id).collect();
        let account_projections: Vec<AccountProjection> =
            projections.into_iter().map(|(_, p)| p).collect();

        self.projection_store
            .bulk_update_accounts_with_copy(account_projections)
            .await?;

        Ok(operation_ids)
    }

    // NEW: Bulk transaction creation submission
    pub async fn submit_transaction_creations_bulk(
        &self,
        transactions: Vec<TransactionProjection>,
    ) -> Result<Vec<Uuid>> {
        tracing::debug!("Submitting transaction creations in bulk");
        if transactions.is_empty() {
            return Ok(vec![]);
        }
        let operation_ids = transactions.iter().map(|t| t.id).collect();
        self.projection_store
            .bulk_insert_transaction_projections(transactions)
            .await?;
        Ok(operation_ids)
    }

    // OPTIMIZATION 21: Get comprehensive metrics
    pub fn get_performance_metrics(&self) -> String {
        let (created, updated, transactions, batches) = self.metrics.get_totals();

        format!(
            "CDC Metrics: {} accounts created, {} accounts updated, {} transactions created, {} batches processed",
            created, updated, transactions, batches
        )
    }
}
