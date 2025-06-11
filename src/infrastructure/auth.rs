use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use redis::{aio::Connection, AsyncCommands, RedisError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("Token expired")]
    TokenExpired,
    #[error("Invalid token")]
    InvalidToken,
    #[error("Token blacklisted")]
    TokenBlacklisted,
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
    #[error("User not found")]
    UserNotFound,
    #[error("Password hash error: {0}")]
    PasswordHashError(String),
    #[error("Redis error: {0}")]
    RedisError(#[from] RedisError),
    #[error("JWT error: {0}")]
    JwtError(#[from] jsonwebtoken::errors::Error),
    #[error("Internal error: {0}")]
    InternalError(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub exp: i64,
    pub iat: i64,
    pub roles: Vec<UserRole>,
    pub token_type: TokenType,
    pub jti: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum TokenType {
    Access,
    Refresh,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum UserRole {
    Admin,
    BankManager,
    Customer,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub username: String,
    pub password_hash: String,
    pub roles: Vec<UserRole>,
    pub is_active: bool,
    pub last_login: Option<DateTime<Utc>>,
    pub failed_login_attempts: u32,
    pub locked_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthConfig {
    pub jwt_secret: String,
    pub refresh_token_secret: String,
    pub access_token_expiry: i64,
    pub refresh_token_expiry: i64,
    pub rate_limit_requests: u32,
    pub rate_limit_window: u32,
    pub max_failed_attempts: u32,
    pub lockout_duration_minutes: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LoginResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub token_type: String,
    pub expires_in: i64,
    pub user_id: String,
    pub roles: Vec<UserRole>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RefreshTokenRequest {
    pub refresh_token: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChangePasswordRequest {
    pub current_password: String,
    pub new_password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PasswordResetRequest {
    pub email: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PasswordResetResponse {
    pub reset_token: String,
    pub expires_in: i64,
}

pub struct AuthService {
    redis_client: Arc<redis::Client>,
    config: AuthConfig,
    users: Arc<RwLock<Vec<User>>>,
}

impl AuthService {
    pub fn new(redis_client: Arc<redis::Client>, config: AuthConfig) -> Self {
        Self {
            redis_client,
            config,
            users: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn register_user(
        &self,
        username: &str,
        password: &str,
        roles: Vec<UserRole>,
    ) -> Result<User, AuthError> {
        let mut users = self.users.write().await;
        
        // Check if username already exists
        if users.iter().any(|u| u.username == username) {
            return Err(AuthError::InternalError("Username already exists".into()));
        }

        // Hash password
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| AuthError::PasswordHashError(e.to_string()))?
            .to_string();

        let user = User {
            id: Uuid::new_v4().to_string(),
            username: username.to_string(),
            password_hash,
            roles,
            is_active: true,
            last_login: None,
            failed_login_attempts: 0,
            locked_until: None,
        };

        users.push(user.clone());
        Ok(user)
    }

    pub async fn login(&self, username: &str, password: &str) -> Result<LoginResponse, AuthError> {
        let mut users = self.users.write().await;
        let user = users
            .iter_mut()
            .find(|u| u.username == username)
            .ok_or(AuthError::UserNotFound)?;

        // Check if account is locked
        if let Some(locked_until) = user.locked_until {
            if Utc::now() < locked_until {
                return Err(AuthError::InternalError("Account is locked".into()));
            }
            user.locked_until = None;
            user.failed_login_attempts = 0;
        }

        // Verify password
        let parsed_hash = PasswordHash::new(&user.password_hash)
            .map_err(|e| AuthError::PasswordHashError(e.to_string()))?;
        
        if !Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
        {
            user.failed_login_attempts += 1;
            if user.failed_login_attempts >= self.config.max_failed_attempts {
                user.locked_until = Some(Utc::now() + Duration::minutes(self.config.lockout_duration_minutes as i64));
                return Err(AuthError::InternalError("Account locked due to too many failed attempts".into()));
            }
            return Err(AuthError::InvalidCredentials);
        }

        // Reset failed attempts and update last login
        user.failed_login_attempts = 0;
        user.last_login = Some(Utc::now());

        // Generate tokens
        let access_token = self.generate_token(username, &user.roles, TokenType::Access).await?;
        let refresh_token = self.generate_token(username, &user.roles, TokenType::Refresh).await?;

        Ok(LoginResponse {
            access_token,
            refresh_token,
            token_type: "Bearer".to_string(),
            expires_in: self.config.access_token_expiry,
            user_id: user.id.clone(),
            roles: user.roles.clone(),
        })
    }

    pub async fn refresh_token(&self, refresh_token: &str) -> Result<LoginResponse, AuthError> {
        let claims = self.validate_token(refresh_token, TokenType::Refresh).await?;
        
        let users = self.users.read().await;
        let user = users
            .iter()
            .find(|u| u.username == claims.sub)
            .ok_or(AuthError::UserNotFound)?;

        // Generate new tokens
        let access_token = self.generate_token(&user.username, &user.roles, TokenType::Access).await?;
        let new_refresh_token = self.generate_token(&user.username, &user.roles, TokenType::Refresh).await?;

        Ok(LoginResponse {
            access_token,
            refresh_token: new_refresh_token,
            token_type: "Bearer".to_string(),
            expires_in: self.config.access_token_expiry,
            user_id: user.id.clone(),
            roles: user.roles.clone(),
        })
    }

    pub async fn change_password(
        &self,
        username: &str,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), AuthError> {
        let mut users = self.users.write().await;
        let user = users
            .iter_mut()
            .find(|u| u.username == username)
            .ok_or(AuthError::UserNotFound)?;

        // Verify current password
        let parsed_hash = PasswordHash::new(&user.password_hash)
            .map_err(|e| AuthError::PasswordHashError(e.to_string()))?;
        
        if !Argon2::default()
            .verify_password(current_password.as_bytes(), &parsed_hash)
            .is_ok()
        {
            return Err(AuthError::InvalidCredentials);
        }

        // Hash new password
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let new_password_hash = argon2
            .hash_password(new_password.as_bytes(), &salt)
            .map_err(|e| AuthError::PasswordHashError(e.to_string()))?
            .to_string();

        user.password_hash = new_password_hash;
        Ok(())
    }

    pub async fn request_password_reset(&self, email: &str) -> Result<PasswordResetResponse, AuthError> {
        let users = self.users.read().await;
        let user = users
            .iter()
            .find(|u| u.username == email)
            .ok_or(AuthError::UserNotFound)?;

        // Generate reset token
        let reset_token = self.generate_token(&user.username, &user.roles, TokenType::Access).await?;
        
        // Store reset token in Redis with expiration
        let mut conn = self.redis_client.get_async_connection().await?;
        conn.set_ex(
            format!("reset_token:{}", user.id),
            &reset_token,
            self.config.access_token_expiry as usize,
        )
        .await?;

        Ok(PasswordResetResponse {
            reset_token,
            expires_in: self.config.access_token_expiry,
        })
    }

    async fn generate_token(
        &self,
        username: &str,
        roles: &[UserRole],
        token_type: TokenType,
    ) -> Result<String, AuthError> {
        let now = Utc::now();
        let exp = match token_type {
            TokenType::Access => now + Duration::seconds(self.config.access_token_expiry),
            TokenType::Refresh => now + Duration::seconds(self.config.refresh_token_expiry),
        };

        let claims = Claims {
            sub: username.to_string(),
            exp: exp.timestamp(),
            iat: now.timestamp(),
            roles: roles.to_vec(),
            token_type,
            jti: Uuid::new_v4().to_string(),
        };

        let secret = match token_type {
            TokenType::Access => &self.config.jwt_secret,
            TokenType::Refresh => &self.config.refresh_token_secret,
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .map_err(AuthError::from)
    }

    pub async fn validate_token(&self, token: &str, expected_type: TokenType) -> Result<Claims, AuthError> {
        // Check if token is blacklisted
        let mut conn = self.redis_client.get_async_connection().await?;
        let is_blacklisted: bool = conn
            .get(format!("blacklist:{}", token))
            .await
            .unwrap_or(false);

        if is_blacklisted {
            return Err(AuthError::TokenBlacklisted);
        }

        // Validate token
        let secret = match expected_type {
            TokenType::Access => &self.config.jwt_secret,
            TokenType::Refresh => &self.config.refresh_token_secret,
        };

        let validation = Validation {
            validate_exp: true,
            ..Validation::default()
        };

        let token_data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(secret.as_bytes()),
            &validation,
        )?;

        if token_data.claims.token_type != expected_type {
            return Err(AuthError::InvalidToken);
        }

        Ok(token_data.claims)
    }

    pub async fn blacklist_token(&self, token: &str) -> Result<(), AuthError> {
        let claims = self.validate_token(token, TokenType::Access).await?;
        let mut conn = self.redis_client.get_async_connection().await?;

        // Blacklist token until it expires
        let ttl = claims.exp - Utc::now().timestamp();
        if ttl > 0 {
            conn.set_ex(format!("blacklist:{}", token), true, ttl as usize)
                .await?;
        }

        Ok(())
    }

    pub async fn check_rate_limit(&self, key: &str) -> Result<(), AuthError> {
        let mut conn = self.redis_client.get_async_connection().await?;
        let current = conn
            .incr(format!("rate_limit:{}", key), 1)
            .await?;

        if current == 1 {
            conn.expire(format!("rate_limit:{}", key), self.config.rate_limit_window as usize)
                .await?;
        }

        if current > self.config.rate_limit_requests {
            return Err(AuthError::RateLimitExceeded);
        }

        Ok(())
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let auth_header = parts
            .headers
            .get("Authorization")
            .ok_or(AuthError::InvalidToken)?
            .to_str()
            .map_err(|_| AuthError::InvalidToken)?;

        if !auth_header.starts_with("Bearer ") {
            return Err(AuthError::InvalidToken);
        }

        let token = auth_header.trim_start_matches("Bearer ");
        let auth_service = parts
            .extensions
            .get::<Arc<AuthService>>()
            .ok_or_else(|| AuthError::InternalError("Auth service not found".into()))?;

        auth_service.validate_token(token, TokenType::Access).await
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AuthError::InvalidCredentials => (StatusCode::UNAUTHORIZED, "Invalid credentials"),
            AuthError::TokenExpired => (StatusCode::UNAUTHORIZED, "Token expired"),
            AuthError::InvalidToken => (StatusCode::UNAUTHORIZED, "Invalid token"),
            AuthError::TokenBlacklisted => (StatusCode::UNAUTHORIZED, "Token has been revoked"),
            AuthError::RateLimitExceeded => (StatusCode::TOO_MANY_REQUESTS, "Rate limit exceeded"),
            AuthError::UserNotFound => (StatusCode::NOT_FOUND, "User not found"),
            AuthError::PasswordHashError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Password processing error"),
            AuthError::RedisError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Database error"),
            AuthError::JwtError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Token processing error"),
            AuthError::InternalError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.as_str()),
        };

        let body = Json(serde_json::json!({
            "error": error_message
        }));

        (status, body).into_response()
    }
} 