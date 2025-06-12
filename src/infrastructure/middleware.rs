use crate::domain::Account;
use anyhow::Result;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use uuid::Uuid;
use validator::{Validate, ValidationError};
use std::collections::HashMap;
use axum::{
    http::{Request, Response, HeaderMap, StatusCode},
    middleware::Next,
};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Service;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct RateLimiter {
    limits: Arc<DashMap<String, RateLimitInfo>>,
    config: RateLimitConfig,
}

#[derive(Debug, Clone)]
struct RateLimitInfo {
    requests: u32,
    window_start: Instant,
    semaphore: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub requests_per_minute: u32,
    pub burst_size: u32,
    pub window_size: Duration,
    pub max_clients: usize,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_minute: 60,
            burst_size: 10,
            window_size: Duration::from_secs(60),
            max_clients: 10000,
        }
    }
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            limits: Arc::new(DashMap::new()),
            config,
        }
    }

    pub async fn check_rate_limit(&self, client_id: &str) -> Result<bool> {
        let now = Instant::now();
        let mut limit_info = self.limits.entry(client_id.to_string()).or_insert_with(|| {
            RateLimitInfo {
                requests: 0,
                window_start: now,
                semaphore: Arc::new(Semaphore::new(self.config.burst_size as usize)),
            }
        });

        // Reset window if needed
        if now.duration_since(limit_info.window_start) >= self.config.window_size {
            limit_info.requests = 0;
            limit_info.window_start = now;
        }

        // Check if we're within the rate limit
        if limit_info.requests >= self.config.requests_per_minute {
            warn!("Rate limit exceeded for client {}", client_id);
            return Ok(false);
        }

        // Try to acquire semaphore for burst control
        if limit_info.semaphore.try_acquire().is_err() {
            warn!("Burst limit exceeded for client {}", client_id);
            return Ok(false);
        }

        limit_info.requests += 1;
        Ok(true)
    }

    pub fn cleanup_expired_limits(&self) {
        let now = Instant::now();
        self.limits.retain(|_, info| {
            now.duration_since(info.window_start) < self.config.window_size
        });
    }
}

#[derive(Debug, Clone)]
pub struct RequestValidator {
    validators: Arc<DashMap<String, Box<dyn RequestValidationRule + Send + Sync>>>,
}

#[async_trait::async_trait]
pub trait RequestValidationRule: Send + Sync + std::fmt::Debug {
    async fn validate(&self, request: &RequestContext) -> ValidationResult;
}

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub client_id: String,
    pub request_type: String,
    pub payload: Value,
    pub headers: HeaderMap,
}

#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl RequestValidator {
    pub fn new() -> Self {
        Self {
            validators: Arc::new(DashMap::new()),
        }
    }

    pub fn register_validator(
        &self,
        request_type: String,
        validator: Box<dyn RequestValidationRule + Send + Sync>,
    ) {
        self.validators.insert(request_type, validator);
    }

    pub async fn validate_request(&self, context: &RequestContext) -> ValidationResult {
        let mut result = ValidationResult {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        };

        if let Some(validator) = self.validators.get(&context.request_type) {
            let validation = validator.validate(context).await;
            result.is_valid &= validation.is_valid;
            result.errors.extend(validation.errors);
            result.warnings.extend(validation.warnings);
        }

        result
    }
}

#[derive(Debug)]
pub struct AccountCreationValidator;

#[async_trait::async_trait]
impl RequestValidationRule for AccountCreationValidator {
    async fn validate(&self, context: &RequestContext) -> ValidationResult {
        let mut result = ValidationResult {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        };

        // Validate required fields
        if !context.payload.is_object() {
            result.is_valid = false;
            result.errors.push("Payload must be a JSON object".to_string());
            return result;
        }

        let payload = context.payload.as_object().unwrap();

        // Check owner_name
        if !payload.contains_key("owner_name") {
            result.is_valid = false;
            result.errors.push("owner_name is required".to_string());
        } else if let Some(name) = payload["owner_name"].as_str() {
            if name.is_empty() {
                result.is_valid = false;
                result.errors.push("owner_name cannot be empty".to_string());
            }
        }

        // Check initial_balance
        if !payload.contains_key("initial_balance") {
            result.is_valid = false;
            result.errors.push("initial_balance is required".to_string());
        } else if let Some(balance) = payload["initial_balance"].as_f64() {
            if balance < 0.0 {
                result.is_valid = false;
                result.errors.push("initial_balance cannot be negative".to_string());
            }
        }

        result
    }
}

#[derive(Debug)]
pub struct TransactionValidator;

#[async_trait::async_trait]
impl RequestValidationRule for TransactionValidator {
    async fn validate(&self, context: &RequestContext) -> ValidationResult {
        let mut result = ValidationResult {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        };

        // Validate required fields
        if !context.payload.is_object() {
            result.is_valid = false;
            result.errors.push("Payload must be a JSON object".to_string());
            return result;
        }

        let payload = context.payload.as_object().unwrap();

        // Check account_id
        if !payload.contains_key("account_id") {
            result.is_valid = false;
            result.errors.push("account_id is required".to_string());
        } else if let Some(id) = payload["account_id"].as_str() {
            if let Err(_) = Uuid::parse_str(id) {
                result.is_valid = false;
                result.errors.push("account_id must be a valid UUID".to_string());
            }
        }

        // Check amount
        if !payload.contains_key("amount") {
            result.is_valid = false;
            result.errors.push("amount is required".to_string());
        } else if let Some(amount) = payload["amount"].as_f64() {
            if amount <= 0.0 {
                result.is_valid = false;
                result.errors.push("amount must be greater than zero".to_string());
            }
        }

        result
    }
}

#[derive(Debug)]
pub struct RequestMiddleware {
    rate_limiter: RateLimiter,
    request_validator: RequestValidator,
}

impl RequestMiddleware {
    pub fn new(rate_limit_config: RateLimitConfig) -> Self {
        Self {
            rate_limiter: RateLimiter::new(rate_limit_config),
            request_validator: RequestValidator::new(),
        }
    }

    pub async fn process_request(&self, context: RequestContext) -> Result<ValidationResult> {
        // Check rate limit
        if !self.rate_limiter.check_rate_limit(&context.client_id).await? {
            return Ok(ValidationResult {
                is_valid: false,
                errors: vec!["Rate limit exceeded".to_string()],
                warnings: Vec::new(),
            });
        }

        // Validate request
        let validation_result = self.request_validator.validate_request(&context).await;
        Ok(validation_result)
    }

    pub fn register_validator(
        &self,
        request_type: String,
        validator: Box<dyn RequestValidationRule + Send + Sync>,
    ) {
        self.request_validator.register_validator(request_type, validator);
    }
}

impl Default for RequestMiddleware {
    fn default() -> Self {
        Self::new(RateLimitConfig::default())
    }
} 