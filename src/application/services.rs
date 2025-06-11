use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::domain::{Account, AccountCommand, AccountError, AccountEvent};
use crate::infrastructure::projections::{AccountProjection, TransactionProjection};
use crate::infrastructure::repository::AccountRepositoryTrait;
use crate::infrastructure::{AccountRepository, EventStore, EventStoreConfig, ProjectionStore, CacheService};
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// Service metrics
#[derive(Debug, Default)]
struct ServiceMetrics {
    command_processing_time: std::sync::atomic::AtomicU64,
    commands_processed: std::sync::atomic::AtomicU64,
    commands_failed: std::sync::atomic::AtomicU64,
    projection_updates: std::sync::atomic::AtomicU64,
    projection_errors: std::sync::atomic::AtomicU64,
    cache_hits: std::sync::atomic::AtomicU64,
    cache_misses: std::sync::atomic::AtomicU64,
}

pub struct AccountService {
    repository: Arc<dyn AccountRepositoryTrait + 'static>,
    projections: ProjectionStore,
    cache_service: CacheService,
    metrics: Arc<ServiceMetrics>,
    command_cache: Arc<RwLock<std::collections::HashMap<Uuid, Instant>>>,
}

impl AccountService {
    /// Creates a new `AccountService`.
    ///
    /// # Arguments
    ///
    /// * `repository`: The account repository for data access.
    /// * `projections`: The projection store for querying denormalized views.
    /// * `cache_service`: The cache service for caching account data.
    pub fn new(
        repository: Arc<dyn AccountRepositoryTrait + 'static>,
        projections: ProjectionStore,
        cache_service: CacheService,
    ) -> Self {
        let service = Self {
            repository,
            projections,
            cache_service,
            metrics: Arc::new(ServiceMetrics::default()),
            command_cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
        };

        // Start metrics reporter
        let metrics = service.metrics.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let processed = metrics
                    .commands_processed
                    .load(std::sync::atomic::Ordering::Relaxed);
                let failed = metrics
                    .commands_failed
                    .load(std::sync::atomic::Ordering::Relaxed);
                let avg_processing_time = metrics
                    .command_processing_time
                    .load(std::sync::atomic::Ordering::Relaxed)
                    as f64
                    / 1000.0; // Convert to milliseconds
                let projection_updates = metrics
                    .projection_updates
                    .load(std::sync::atomic::Ordering::Relaxed);
                let projection_errors = metrics
                    .projection_errors
                    .load(std::sync::atomic::Ordering::Relaxed);

                let success_rate = if processed > 0 {
                    ((processed - failed) as f64 / processed as f64) * 100.0
                } else {
                    0.0
                };

                info!(
                    "Account Service Metrics - Commands: {} ({}% success), Avg Processing Time: {:.2}ms, Projection Updates: {}, Errors: {}",
                    processed, success_rate, avg_processing_time, projection_updates, projection_errors
                );
            }
        });

        service
    }

    pub async fn create_account(
        &self,
        owner_name: String,
        initial_balance: Decimal,
    ) -> Result<Uuid, AccountError> {
        let start_time = Instant::now();
        let account_id = Uuid::new_v4();

        // Check for duplicate command
        if self.is_duplicate_command(account_id).await {
            return Err(AccountError::InfrastructureError(
                "Duplicate create account command".to_string(),
            ));
        }

        let command = AccountCommand::CreateAccount {
            account_id,
            owner_name: owner_name.clone(),
            initial_balance,
        };

        let account = Account::default();
        let events = account.handle_command(&command)?;

        // Save events with retry logic
        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|e| {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AccountError::InfrastructureError(e.to_string())
            })?;

        // Update projections
        let projection = AccountProjection {
            id: account_id,
            owner_name,
            balance: initial_balance,
            is_active: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        if let Err(e) = self
            .projections
            .upsert_accounts_batch(vec![projection])
            .await
        {
            self.metrics
                .projection_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!(
                "Failed to update projection for account {}: {}",
                account_id, e
            );
        } else {
            self.metrics
                .projection_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.command_processing_time.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(account_id)
    }

    pub async fn deposit_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<(), AccountError> {
        let start_time = Instant::now();

        // Check for duplicate command
        if self.is_duplicate_command(account_id).await {
            return Err(AccountError::InfrastructureError(
                "Duplicate deposit command".to_string(),
            ));
        }

        let mut account = self
            .repository
            .get_by_id(account_id)
            .await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::DepositMoney { account_id, amount };
        let events = account.handle_command(&command)?;

        // Apply events to account
        for event in &events {
            account.apply_event(event);
        }

        // Save events with retry logic
        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|e| {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AccountError::InfrastructureError(e.to_string())
            })?;

        // Update projections
        if let Err(e) = self.update_projections_from_events(&events).await {
            self.metrics
                .projection_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!("Failed to update projections for deposit: {}", e);
        } else {
            self.metrics
                .projection_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.command_processing_time.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(())
    }

    pub async fn withdraw_money(
        &self,
        account_id: Uuid,
        amount: Decimal,
    ) -> Result<(), AccountError> {
        let start_time = Instant::now();

        // Check for duplicate command
        if self.is_duplicate_command(account_id).await {
            return Err(AccountError::InfrastructureError(
                "Duplicate withdraw command".to_string(),
            ));
        }

        let mut account = self
            .repository
            .get_by_id(account_id)
            .await?
            .ok_or(AccountError::NotFound)?;

        let command = AccountCommand::WithdrawMoney { account_id, amount };
        let events = account.handle_command(&command)?;

        // Apply events to account
        for event in &events {
            account.apply_event(event);
        }

        // Save events with retry logic
        self.repository
            .save(&account, events.clone())
            .await
            .map_err(|e| {
                self.metrics
                    .commands_failed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AccountError::InfrastructureError(e.to_string())
            })?;

        // Update projections
        if let Err(e) = self.update_projections_from_events(&events).await {
            self.metrics
                .projection_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!("Failed to update projections for withdrawal: {}", e);
        } else {
            self.metrics
                .projection_updates
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.metrics
            .commands_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.command_processing_time.fetch_add(
            start_time.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(())
    }

    pub async fn get_account(
        &self,
        account_id: Uuid,
    ) -> Result<Option<AccountProjection>, AccountError> {
        // Try cache first
        if let Some(account) = self.cache_service.get_account(account_id).await? {
            self.metrics.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(Some(AccountProjection {
                id: account.id,
                owner_name: account.owner_name,
                balance: account.balance,
                is_active: account.is_active,
                version: account.version,
            }));
        }
        self.metrics.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Fall back to projections
        self.projections.get_account(account_id).await
    }

    pub async fn get_all_accounts(&self) -> Result<Vec<AccountProjection>, AccountError> {
        self.projections
            .get_all_accounts()
            .await
            .map_err(|e| AccountError::InfrastructureError(e.to_string()))
    }

    pub async fn get_account_transactions(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<TransactionProjection>, AccountError> {
        // Try cache first
        if let Some(events) = self.cache_service.get_account_events(account_id).await? {
            self.metrics.cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(events.into_iter().map(|e| e.into()).collect());
        }
        self.metrics.cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Fall back to projections
        self.projections.get_account_transactions(account_id).await
    }

    async fn update_projections_from_events(
        &self,
        events: &[crate::domain::AccountEvent],
    ) -> Result<(), AccountError> {
        let start_time = Instant::now();
        
        for event in events {
            match self.projections.update_from_event(event).await {
                Ok(_) => {
                    self.metrics.projection_updates.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    
                    // Update cache with new account state
                    if let Some(account) = self.projections.get_account(event.get_account_id()).await? {
                        let account = Account {
                            id: account.id,
                            owner_name: account.owner_name,
                            balance: account.balance,
                            is_active: account.is_active,
                            version: account.version,
                        };
                        self.cache_service
                            .set_account(&account, Some(Duration::from_secs(3600)))
                            .await?;
                    }
                }
                Err(e) => {
                    self.metrics.projection_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    error!("Failed to update projection: {}", e);
                    return Err(AccountError::ProjectionError(e));
                }
            }
        }

        let duration = start_time.elapsed();
        debug!("Projection update took {:?}", duration);
        Ok(())
    }

    pub async fn is_duplicate_command(&self, command_id: Uuid) -> bool {
        self.command_cache.read().await.contains_key(&command_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::AccountError;
    use crate::infrastructure::repository::AccountRepositoryTrait;
    use crate::infrastructure::{AccountRepository, EventStore, EventStoreConfig, ProjectionStore};
    use async_trait::async_trait;
    use uuid::Uuid;

    // Manual mock for AccountRepositoryTrait
    #[derive(Clone)]
    struct ManualMockAccountRepository {
        pub save_batched_result: Arc<Result<(), AccountError>>,
        pub get_by_id_result: Arc<Result<Option<Account>, AccountError>>,
    }

    #[async_trait]
    impl AccountRepositoryTrait for ManualMockAccountRepository {
        async fn save(&self, _account: &Account, _events: Vec<AccountEvent>) -> Result<()> {
            match &*self.save_batched_result {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow::anyhow!(e.to_string())),
            }
        }

        async fn get_by_id(&self, _id: Uuid) -> Result<Option<Account>, AccountError> {
            match &*self.get_by_id_result {
                Ok(account) => Ok(account.clone()),
                Err(e) => Err(e.clone()),
            }
        }

        async fn save_batched(
            &self,
            _account_id: Uuid,
            _expected_version: i64,
            _events: Vec<AccountEvent>,
        ) -> Result<()> {
            match &*self.save_batched_result {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow::anyhow!(e.to_string())),
            }
        }

        async fn save_immediate(
            &self,
            _account: &Account,
            _events: Vec<AccountEvent>,
        ) -> Result<()> {
            match &*self.save_batched_result {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow::anyhow!(e.to_string())),
            }
        }

        async fn flush_all(&self) -> Result<()> {
            Ok(())
        }

        fn start_batch_flush_task(&self) {}
    }

    fn account_service_with_mock_repo(
        mock_repo: Arc<dyn AccountRepositoryTrait + 'static>,
    ) -> AccountService {
        let ps_pool = tokio::runtime::Runtime::new().unwrap().block_on(async {
            sqlx::PgPool::connect("postgres://postgres:postgres@localhost:5432/banking_test")
                .await
                .unwrap()
        });
        let projection_store = ProjectionStore::new(ps_pool);

        AccountService::new(mock_repo, projection_store)
    }

    #[tokio::test]
    async fn test_is_duplicate_command_new_command() {
        let account_id = Uuid::new_v4();
        let mock_repo = ManualMockAccountRepository {
            save_batched_result: Arc::new(Ok(())),
            get_by_id_result: Arc::new(Ok(None)),
        };

        let service = account_service_with_mock_repo(Arc::new(mock_repo));

        let result = service.is_duplicate_command(account_id).await;
        assert!(!result, "New command should not be detected as duplicate");
    }

    #[tokio::test]
    async fn test_is_duplicate_command_duplicate_detected() {
        let account_id_for_deposit = Uuid::new_v4();

        let mock_repo = ManualMockAccountRepository {
            save_batched_result: Arc::new(Ok(())),
            get_by_id_result: Arc::new(Ok(None)),
        };

        let service = account_service_with_mock_repo(Arc::new(mock_repo));

        // First call should not be a duplicate
        let first_result = service.is_duplicate_command(account_id_for_deposit).await;
        assert!(
            !first_result,
            "First call should not be detected as duplicate"
        );

        // Second call should be a duplicate
        let second_result = service.is_duplicate_command(account_id_for_deposit).await;
        assert!(second_result, "Second call should be detected as duplicate");
    }
}
