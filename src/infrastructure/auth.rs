use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use async_trait::async_trait;
use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json, RequestPartsExt,
};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc}; // Renamed std::time::Duration to avoid conflict
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use redis::{AsyncCommands, RedisError};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;
use thiserror::Error;
use uuid::Uuid;
use strum_macros::{EnumString, ToString};

use crate::infrastructure::user_repository::{
    User, UserRepository, NewUser, UserRepositoryError,
};

static KEYS: LazyLock<Keys> = LazyLock::new(|| {
    let secret = std::env::var("JWT_SECRET").expect("JWT_SECRET must be set");
    Keys::new(secret.as_bytes())
});

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
    #[error("Username '{0}' already exists")]
    UsernameAlreadyExists(String),
    #[error("Email '{0}' already exists")]
    EmailAlreadyExists(String),
    #[error("Password hash error: {0}")]
    PasswordHashError(String),
    #[error("Redis error: {0}")]
    RedisError(#[from] RedisError),
    #[error("JWT error: {0}")]
    JwtError(#[from] jsonwebtoken::errors::Error),
    #[error("User repository error: {0}")]
    UserRepositoryError(String), // To wrap UserRepositoryError
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Wrong credentials")]
    WrongCredentials,
    #[error("Missing credentials")]
    MissingCredentials,
    #[error("Token creation error")]
    TokenCreation,
    #[error("Account is locked")]
    AccountLocked,
}

impl From<UserRepositoryError> for AuthError {
    fn from(err: UserRepositoryError) -> Self {
        match err {
            UserRepositoryError::NotFound => AuthError::UserNotFound,
            UserRepositoryError::UsernameExists(username) => AuthError::UsernameAlreadyExists(username),
            UserRepositoryError::EmailExists(email) => AuthError::EmailAlreadyExists(email),
            UserRepositoryError::DatabaseError(db_err) => AuthError::UserRepositoryError(db_err.to_string()),
            UserRepositoryError::Unexpected(msg) => AuthError::InternalError(msg),
        }
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String, // username
    pub exp: i64,
    pub iat: i64,
    pub roles: Vec<UserRole>,
    pub token_type: TokenType,
    pub jti: String, // JWT ID
    pub company: String, // Example custom claim
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy, ToString, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum TokenType {
    Access,
    Refresh,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, ToString, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum UserRole {
    Admin,
    BankManager,
    Customer,
}

// The User struct is now imported from user_repository

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub jwt_secret: String,
    pub refresh_token_secret: String,
    pub access_token_expiry: i64,    // in seconds
    pub refresh_token_expiry: i64,   // in seconds
    pub rate_limit_requests: u32,
    pub rate_limit_window: u32,      // in seconds
    pub max_failed_attempts: i32,    // Now i32
    pub lockout_duration_minutes: i64, // Now i64
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
    pub token_type: String, // Typically "Bearer"
    pub expires_in: i64,    // Access token expiry in seconds
    pub user_id: String,    // User's UUID as string
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

// PasswordReset related structs can remain the same for now
#[derive(Debug, Serialize, Deserialize)]
pub struct PasswordResetRequest {
    pub email: String, // Assuming email is used for password reset initiation
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PasswordResetResponse {
    pub reset_token: String,
    pub expires_in: i64, // Reset token expiry
}


#[derive(Debug, Serialize, Deserialize)]
pub struct LogoutRequest {
    pub token: String, // The access token to blacklist
}


#[derive(Clone)]
pub struct AuthService {
    redis_client: Arc<redis::Client>,
    config: AuthConfig,
    user_repository: Arc<UserRepository>,
}

impl AuthService {
    pub fn new(
        redis_client: Arc<redis::Client>,
        config: AuthConfig,
        user_repository: Arc<UserRepository>,
    ) -> Self {
        Self {
            redis_client,
            config,
            user_repository,
        }
    }

    pub async fn register_user(
        &self,
        username: String,
        email: Option<String>, // Added email
        password: &str,
        roles: Vec<UserRole>,
    ) -> Result<User, AuthError> {
        // Hash password
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| AuthError::PasswordHashError(e.to_string()))?
            .to_string();

        let new_user_data = NewUser {
            username,
            email,
            password_hash,
            roles: roles.iter().map(|r| r.to_string()).collect(),
            is_active: Some(true),
        };

        let user = self.user_repository.create(&new_user_data).await?;
        Ok(user)
    }

    pub async fn login(&self, username: &str, password: &str) -> Result<LoginResponse, AuthError> {
        let user = self
            .user_repository
            .find_by_username(username)
            .await?
            .ok_or(AuthError::UserNotFound)?;

        if !user.is_active {
            return Err(AuthError::InternalError("Account is inactive".into()));
        }

        if let Some(locked_until_ts) = user.locked_until {
            if Utc::now() < locked_until_ts {
                return Err(AuthError::AccountLocked);
            }
            // Lock expired, reset it by calling update_lockout
            self.user_repository
                .update_lockout(user.id, None, 0) // Reset failed attempts and clear lock
                .await?;
        }

        let parsed_hash = PasswordHash::new(&user.password_hash)
            .map_err(|e| AuthError::PasswordHashError(e.to_string()))?;

        if !Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
        {
            let new_failed_attempts = self
                .user_repository
                .increment_failed_attempts(user.id)
                .await?;

            if new_failed_attempts >= self.config.max_failed_attempts {
                let locked_until = Utc::now() + ChronoDuration::minutes(self.config.lockout_duration_minutes);
                self.user_repository
                    .update_lockout(user.id, Some(locked_until), new_failed_attempts)
                    .await?;
                return Err(AuthError::AccountLocked);
            }
            return Err(AuthError::InvalidCredentials);
        }

        // Reset failed attempts and update last login on successful login
        self.user_repository
            .update_login_info(user.id, Utc::now(), 0, None)
            .await?;

        let user_roles: Vec<UserRole> = user.roles.iter()
            .map(|s| UserRole::from_str(s).unwrap_or(UserRole::Customer)) // Default to Customer on parse error
            .collect();

        let access_token = self
            .generate_token(&user.username, &user_roles, TokenType::Access)
            .await?;
        let refresh_token = self
            .generate_token(&user.username, &user_roles, TokenType::Refresh)
            .await?;

        Ok(LoginResponse {
            access_token,
            refresh_token,
            token_type: "Bearer".to_string(),
            expires_in: self.config.access_token_expiry,
            user_id: user.id.to_string(),
            roles: user_roles,
        })
    }

    pub async fn refresh_token(&self, refresh_token_str: &str) -> Result<LoginResponse, AuthError> {
        let claims = self
            .validate_token(refresh_token_str, TokenType::Refresh)
            .await?;

        let user = self
            .user_repository
            .find_by_username(&claims.sub) // claims.sub stores the username
            .await?
            .ok_or(AuthError::UserNotFound)?;

        if !user.is_active {
             return Err(AuthError::InternalError("Account is inactive".into()));
        }

        let user_roles: Vec<UserRole> = user.roles.iter()
            .map(|s| UserRole::from_str(s).unwrap_or(UserRole::Customer))
            .collect();

        let access_token = self
            .generate_token(&user.username, &user_roles, TokenType::Access)
            .await?;
        let new_refresh_token = self
            .generate_token(&user.username, &user_roles, TokenType::Refresh)
            .await?;

        Ok(LoginResponse {
            access_token,
            refresh_token: new_refresh_token,
            token_type: "Bearer".to_string(),
            expires_in: self.config.access_token_expiry,
            user_id: user.id.to_string(),
            roles: user_roles,
        })
    }

    pub async fn change_password(
        &self,
        username: &str, // Or user_id from claims
        current_password: &str,
        new_password: &str,
    ) -> Result<(), AuthError> {
        let user = self
            .user_repository
            .find_by_username(username)
            .await?
            .ok_or(AuthError::UserNotFound)?;

        let parsed_hash = PasswordHash::new(&user.password_hash)
            .map_err(|e| AuthError::PasswordHashError(e.to_string()))?;

        if !Argon2::default()
            .verify_password(current_password.as_bytes(), &parsed_hash)
            .is_ok()
        {
            return Err(AuthError::InvalidCredentials);
        }

        let new_salt = SaltString::generate(&mut OsRng);
        let new_password_hash = Argon2::default()
            .hash_password(new_password.as_bytes(), &new_salt)
            .map_err(|e| AuthError::PasswordHashError(e.to_string()))?
            .to_string();

        self.user_repository
            .update_password_hash(user.id, &new_password_hash)
            .await?;
        Ok(())
    }

    // request_password_reset and other methods like find_user_by_id would also use user_repository
    // For brevity, only core methods are fully refactored here.

    async fn generate_token(
        &self,
        username: &str,
        roles: &[UserRole], // Takes UserRole slice
        token_type: TokenType,
    ) -> Result<String, AuthError> {
        let now = Utc::now();
        let exp_duration = match token_type {
            TokenType::Access => ChronoDuration::seconds(self.config.access_token_expiry),
            TokenType::Refresh => ChronoDuration::seconds(self.config.refresh_token_expiry),
        };
        let exp = (now + exp_duration).timestamp();

        let claims = Claims {
            sub: username.to_string(),
            exp,
            iat: now.timestamp(),
            roles: roles.to_vec(), // Clones Vec<UserRole>
            token_type,
            jti: Uuid::new_v4().to_string(),
            company: "ACME".to_string(), // Example claim
        };

        let secret_key = match token_type {
            TokenType::Access => &self.config.jwt_secret,
            TokenType::Refresh => &self.config.refresh_token_secret,
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret_key.as_bytes()),
        )
        .map_err(AuthError::JwtError)
    }

    pub async fn validate_token(
        &self,
        token: &str,
        expected_type: TokenType,
    ) -> Result<Claims, AuthError> {
        let mut conn = self.redis_client.get_async_connection().await?;
        let is_blacklisted: Option<bool> = conn.get(format!("blacklist:{}", token)).await?;
        if is_blacklisted.unwrap_or(false) {
            return Err(AuthError::TokenBlacklisted);
        }

        let secret_key = match expected_type {
            TokenType::Access => &self.config.jwt_secret,
            TokenType::Refresh => &self.config.refresh_token_secret,
        };
        let decoding_key = DecodingKey::from_secret(secret_key.as_bytes());
        let validation = Validation::new(Algorithm::HS256);

        let token_data = decode::<Claims>(token, &decoding_key, &validation)?;

        if token_data.claims.token_type != expected_type {
            return Err(AuthError::InvalidToken);
        }

        Ok(token_data.claims)
    }

    pub async fn blacklist_token(&self, token: &str) -> Result<(), AuthError> {
        // Validate that it's an access token before blacklisting based on its own expiry
        let claims = self.validate_token(token, TokenType::Access).await?;
        let mut conn = self.redis_client.get_async_connection().await?;
        let now = Utc::now().timestamp();
        let ttl = claims.exp - now;

        if ttl > 0 {
            conn.set_ex(format!("blacklist:{}", token), true, ttl as usize)
                .await?;
        }
        Ok(())
    }

    pub async fn check_rate_limit(&self, key: &str) -> Result<(), AuthError> {
        let mut conn = self.redis_client.get_async_connection().await?;
        let count_key = format!("rate_limit:{}", key);
        let current_count: i64 = conn.incr(&count_key, 1).await?;

        if current_count == 1 {
            // First request in window, set expiry
            conn.expire(&count_key, self.config.rate_limit_window as usize)
                .await?;
        }

        if current_count > self.config.rate_limit_requests as i64 {
            return Err(AuthError::RateLimitExceeded);
        }
        Ok(())
    }
}


// The Default impl for AuthService is removed as UserRepository cannot be easily defaulted.
// Test setups or main.rs will need to construct AuthService with a UserRepository.

#[async_trait]
impl<S> FromRequestParts<S> for Claims
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| AuthError::InvalidToken)?;

        // For simplicity, assuming validate_token is not directly available on state here.
        // In a real app, you'd get AuthService from state and call validate_token.
        // This simplified version uses the static KEYS.
        let token_data = decode::<Claims>(
                bearer.token(),
                &KEYS.decoding,
                &Validation::new(Algorithm::HS256)
            )
            .map_err(|_| AuthError::InvalidToken)?;

        // Additional check: ensure it's an access token if this extractor is for protected routes
        if token_data.claims.token_type != TokenType::Access {
            return Err(AuthError::InvalidToken);
        }

        Ok(token_data.claims)
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, error_message) = match &self {
            AuthError::InvalidCredentials => (StatusCode::UNAUTHORIZED, "Invalid credentials".to_string()),
            AuthError::TokenExpired => (StatusCode::UNAUTHORIZED, "Token expired".to_string()),
            AuthError::InvalidToken => (StatusCode::UNAUTHORIZED, "Invalid token".to_string()),
            AuthError::TokenBlacklisted => (StatusCode::UNAUTHORIZED, "Token has been revoked".to_string()),
            AuthError::RateLimitExceeded => (StatusCode::TOO_MANY_REQUESTS, "Rate limit exceeded".to_string()),
            AuthError::UserNotFound => (StatusCode::NOT_FOUND, "User not found".to_string()),
            AuthError::UsernameAlreadyExists(u) => (StatusCode::CONFLICT, format!("Username '{}' already exists", u)),
            AuthError::EmailAlreadyExists(e) => (StatusCode::CONFLICT, format!("Email '{}' already exists", e)),
            AuthError::PasswordHashError(s) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Password processing error: {}", s)),
            AuthError::RedisError(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Cache/session error: {}", e)),
            AuthError::JwtError(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Token processing error: {}", e)),
            AuthError::UserRepositoryError(s) => (StatusCode::INTERNAL_SERVER_ERROR, format!("User data access error: {}", s)),
            AuthError::InternalError(s) => (StatusCode::INTERNAL_SERVER_ERROR, s.to_string()),
            AuthError::WrongCredentials => (StatusCode::UNAUTHORIZED, "Wrong credentials".to_string()),
            AuthError::MissingCredentials => (StatusCode::BAD_REQUEST, "Missing credentials".to_string()),
            AuthError::TokenCreation => (StatusCode::INTERNAL_SERVER_ERROR, "Token creation error".to_string()),
            AuthError::AccountLocked => (StatusCode::FORBIDDEN, "Account is locked".to_string()),
        };

        let body = axum::Json(serde_json::json!({ "error": error_message }));
        (status, body).into_response()
    }
}

struct Keys {
    encoding: EncodingKey,
    decoding: DecodingKey,
}

impl Keys {
    fn new(secret: &[u8]) -> Self {
        Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
        }
    }
}

// Example handler stubs - these are not part of AuthService but show how it might be used
// These would typically be in src/web/handlers.rs

// pub async fn authorize_handler(Json(payload): Json<AuthPayload>) -> Result<Json<AuthBody>, AuthError> {
//     // This is a placeholder - real authorization would involve the AuthService
//     if payload.client_id.is_empty() || payload.client_secret.is_empty() {
//         return Err(AuthError::MissingCredentials);
//     }
//     if payload.client_id != "foo" || payload.client_secret != "bar" {
//         return Err(AuthError::WrongCredentials);
//     }
//     let claims = Claims { /* ... */ }; // Construct claims
//     let token = encode(&Header::default(), &claims, &KEYS.encoding).map_err(|_| AuthError::TokenCreation)?;
//     Ok(Json(AuthBody::new(token)))
// }

// pub async fn protected_handler(claims: Claims) -> Result<String, AuthError> {
//     Ok(format!("Welcome!\nYour claims: {:?}", claims))
// }

// #[derive(Debug, Deserialize)]
// pub struct AuthPayload { /* ... */ }
// #[derive(Debug, Serialize)]
// pub struct AuthBody { /* ... */ }
// impl AuthBody { /* ... */ }
