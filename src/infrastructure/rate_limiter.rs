use std::time::Duration;
use dashmap::DashMap;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub requests_per_second: u32,
    pub window_size: Duration,
    pub burst_size: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: 1000,
            window_size: Duration::from_secs(60),
            burst_size: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RateLimiter {
    config: RateLimitConfig,
    requests: DashMap<String, Vec<DateTime<Utc>>>,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            requests: DashMap::new(),
        }
    }

    pub fn is_rate_limited(&self, key: &str) -> bool {
        let now = Utc::now();
        let window_start = now - chrono::Duration::from_std(self.config.window_size).unwrap();

        let mut requests = self.requests.entry(key.to_string()).or_insert_with(Vec::new);
        requests.retain(|&time| time >= window_start);

        requests.len() >= self.config.burst_size as usize
    }

    pub fn record_request(&self, key: &str) {
        let now = Utc::now();
        let mut requests = self.requests.entry(key.to_string()).or_insert_with(Vec::new);
        requests.push(now);
    }
} 