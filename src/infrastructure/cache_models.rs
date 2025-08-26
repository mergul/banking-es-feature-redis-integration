use crate::infrastructure::projections::AccountProjection;
use anyhow::Result;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::time::Instant;
use uuid::Uuid;

// Lightweight projection cache entry with business validation
#[derive(Debug, Clone)]
pub struct ProjectionCacheEntry {
    pub data: AccountProjection, // Embed the full AccountProjection
    pub version: u64,
    pub cached_at: Instant, // When this cache entry was created/last updated
    pub last_event_id: Option<Uuid>, // For duplicate detection
}

impl ProjectionCacheEntry {
    pub fn is_expired(&self, ttl: std::time::Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }

    pub fn apply_event(&mut self, event: &crate::domain::AccountEvent) -> Result<()> {
        // Apply event to the embedded projection
        match event {
            crate::domain::AccountEvent::MoneyDeposited { amount, .. } => {
                self.data.balance += *amount;
                self.version += 1;
            }
            crate::domain::AccountEvent::MoneyWithdrawn { amount, .. } => {
                self.data.balance -= *amount;
                self.version += 1;
            }
            crate::domain::AccountEvent::AccountCreated {
                owner_name,
                initial_balance,
                ..
            } => {
                self.data.owner_name = owner_name.clone();
                self.data.balance = *initial_balance;
                self.data.is_active = true;
                self.version += 1;
            }
            crate::domain::AccountEvent::AccountClosed { .. } => {
                self.data.is_active = false;
                self.version += 1;
            }
        }
        Ok(())
    }
}

impl From<AccountProjection> for ProjectionCacheEntry {
    fn from(projection: AccountProjection) -> Self {
        Self {
            data: projection,
            version: 0, // Initial version for the cache entry
            cached_at: Instant::now(),
            last_event_id: None,
        }
    }
}
