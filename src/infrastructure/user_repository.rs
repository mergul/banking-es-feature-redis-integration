use crate::infrastructure::connection_pool_partitioning::{
    OperationType, PartitionedPools, PoolSelector,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize}; // Added for potential future use, not strictly required by sqlx::FromRow
use sqlx::{postgres::PgDatabaseError, FromRow, PgPool};
use std::sync::Arc;
use uuid::Uuid;

// Custom module for bincode-compatible DateTime<Utc> serialization
mod bincode_datetime {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::de::Deserialize;
    use serde::{self, Deserializer, Serializer};

    pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(dt.timestamp())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ts = i64::deserialize(deserializer)?;
        Ok(Utc.timestamp_opt(ts, 0).single().unwrap())
    }

    pub mod option {
        use super::*;
        use chrono::{DateTime, TimeZone, Utc};
        use serde::de::Deserialize;
        pub fn serialize<S>(dt: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match dt {
                Some(dt) => serializer.serialize_some(&dt.timestamp()),
                None => serializer.serialize_none(),
            }
        }
        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let opt = Option::<i64>::deserialize(deserializer)?;
            Ok(opt.map(|ts| Utc.timestamp_opt(ts, 0).single().unwrap()))
        }
    }
}

#[derive(Debug, FromRow, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub username: String,
    pub email: Option<String>,
    pub password_hash: String,
    pub roles: Vec<String>, // Stored as TEXT[] in PostgreSQL
    pub is_active: bool,
    #[serde(with = "bincode_datetime")]
    pub registered_at: DateTime<Utc>,
    #[serde(with = "bincode_datetime::option")]
    pub last_login_at: Option<DateTime<Utc>>,
    pub failed_login_attempts: i32,
    #[serde(with = "bincode_datetime::option")]
    pub locked_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct NewUser<'a> {
    pub username: &'a str,
    pub email: &'a str,
    pub password_hash: String,
    pub roles: Vec<String>,
    // is_active and registered_at will use database defaults or be set by INSERT query
}

#[derive(Debug)]
pub enum UserRepositoryError {
    NotFoundById(Uuid),
    NotFoundByUsername(String),
    UsernameExists(String),
    EmailExists(String),
    DatabaseError(sqlx::Error),
    Unexpected(String), // For any other kind of error
}

impl std::fmt::Display for UserRepositoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserRepositoryError::NotFoundById(id) => f
                .write_str("User not found for ID: ")
                .and_then(|_| f.write_str(&id.to_string())),
            UserRepositoryError::NotFoundByUsername(username) => f
                .write_str("User not found for username: ")
                .and_then(|_| f.write_str(username)),
            UserRepositoryError::UsernameExists(username) => f
                .write_str("Username '")
                .and_then(|_| f.write_str(username))
                .and_then(|_| f.write_str("' already exists")),
            UserRepositoryError::EmailExists(email) => f
                .write_str("Email '")
                .and_then(|_| f.write_str(email))
                .and_then(|_| f.write_str("' already exists")),
            UserRepositoryError::DatabaseError(e) => f
                .write_str("Database error: ")
                .and_then(|_| f.write_str(&e.to_string())),
            UserRepositoryError::Unexpected(msg) => f
                .write_str("An unexpected error occurred: ")
                .and_then(|_| f.write_str(msg)),
        }
    }
}

impl std::error::Error for UserRepositoryError {}

impl From<sqlx::Error> for UserRepositoryError {
    fn from(err: sqlx::Error) -> Self {
        UserRepositoryError::DatabaseError(err)
    }
}

#[derive(Clone)]
pub struct UserRepository {
    pools: Arc<PartitionedPools>,
}

impl UserRepository {
    pub fn new(pools: Arc<PartitionedPools>) -> Self {
        Self { pools }
    }

    pub async fn create(&self, new_user: &NewUser<'_>) -> Result<User, UserRepositoryError> {
        let user_id = Uuid::new_v4(); // Application generates UUID

        let user = sqlx::query_as!(
            User,
            r#"
            INSERT INTO users (id, username, email, password_hash, roles)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING
                id, username, email, password_hash, roles,
                is_active, registered_at, last_login_at,
                failed_login_attempts, locked_until
            "#,
            user_id,
            new_user.username,
            new_user.email,
            new_user.password_hash,
            &new_user.roles // Pass as slice for TEXT[]
        )
        .fetch_one(self.pools.select_pool(OperationType::Write))
        .await
        .map_err(|e: sqlx::Error| {
            if let Some(db_err) = e.as_database_error() {
                if let Some(pg_err) = db_err.try_downcast_ref::<PgDatabaseError>() {
                    if pg_err.code() == "23505" {
                        // unique_violation
                        // Check constraint name to differentiate between username and email
                        if let Some(constraint_name) = pg_err.constraint() {
                            if constraint_name == "users_username_key" {
                                return UserRepositoryError::UsernameExists(
                                    new_user.username.to_string(),
                                );
                            }
                            if constraint_name == "users_email_key" {
                                return UserRepositoryError::EmailExists(
                                    new_user.email.to_string(),
                                );
                            }
                        }
                    }
                }
            }
            UserRepositoryError::DatabaseError(e)
        })?;

        Ok(user)
    }

    pub async fn find_by_username(
        &self,
        username: &str,
    ) -> Result<Option<User>, UserRepositoryError> {
        sqlx::query_as!(
            User,
            r#"
            SELECT
                id, username, email, password_hash, roles,
                is_active, registered_at, last_login_at,
                failed_login_attempts, locked_until
            FROM users
            WHERE username = $1
            "#,
            username
        )
        .fetch_optional(self.pools.select_pool(OperationType::Read))
        .await
        .map_err(UserRepositoryError::DatabaseError)
    }

    pub async fn find_by_id(&self, user_id: Uuid) -> Result<Option<User>, UserRepositoryError> {
        sqlx::query_as!(
            User,
            r#"
            SELECT
                id, username, email, password_hash, roles,
                is_active, registered_at, last_login_at,
                failed_login_attempts, locked_until
            FROM users
            WHERE id = $1
            "#,
            user_id
        )
        .fetch_optional(self.pools.select_pool(OperationType::Read))
        .await
        .map_err(UserRepositoryError::DatabaseError)
    }

    pub async fn update_login_info(
        &self,
        user_id: Uuid,
        last_login_at: DateTime<Utc>,
        failed_login_attempts: i32,
        locked_until: Option<DateTime<Utc>>,
    ) -> Result<(), UserRepositoryError> {
        sqlx::query!(
            r#"
            UPDATE users
            SET last_login_at = $2, failed_login_attempts = $3, locked_until = $4
            WHERE id = $1
            "#,
            user_id,
            last_login_at,
            failed_login_attempts,
            locked_until
        )
        .execute(self.pools.select_pool(OperationType::Write))
        .await
        .map_err(UserRepositoryError::DatabaseError)?;

        Ok(())
    }

    pub async fn update_password_hash(
        &self,
        user_id: Uuid,
        new_password_hash: &str,
    ) -> Result<(), UserRepositoryError> {
        sqlx::query!(
            r#"
            UPDATE users
            SET password_hash = $2
            WHERE id = $1
            "#,
            user_id,
            new_password_hash
        )
        .execute(self.pools.select_pool(OperationType::Write))
        .await
        .map_err(UserRepositoryError::DatabaseError)?;

        Ok(())
    }

    pub async fn update_status(
        &self,
        user_id: Uuid,
        is_active: bool,
    ) -> Result<(), UserRepositoryError> {
        sqlx::query!(
            r#"
            UPDATE users
            SET is_active = $2
            WHERE id = $1
            "#,
            user_id,
            is_active
        )
        .execute(self.pools.select_pool(OperationType::Write))
        .await
        .map_err(UserRepositoryError::DatabaseError)?;

        Ok(())
    }

    pub async fn increment_failed_attempts(
        &self,
        user_id: Uuid,
    ) -> Result<i32, UserRepositoryError> {
        let result = sqlx::query!(
            r#"
            UPDATE users
            SET failed_login_attempts = failed_login_attempts + 1
            WHERE id = $1
            RETURNING failed_login_attempts
            "#,
            user_id
        )
        .fetch_optional(self.pools.select_pool(OperationType::Write))
        .await
        .map_err(UserRepositoryError::DatabaseError)?;

        match result {
            Some(row) => Ok(row.failed_login_attempts),
            None => Err(UserRepositoryError::NotFoundById(user_id)),
        }
    }

    pub async fn update_lockout(
        &self,
        user_id: Uuid,
        locked_until: Option<DateTime<Utc>>,
        failed_attempts_value: i32,
    ) -> Result<(), UserRepositoryError> {
        sqlx::query!(
            r#"
            UPDATE users
            SET locked_until = $2, failed_login_attempts = $3
            WHERE id = $1
            "#,
            user_id,
            locked_until,
            failed_attempts_value
        )
        .execute(self.pools.select_pool(OperationType::Write))
        .await
        .map_err(UserRepositoryError::DatabaseError)?;

        Ok(())
    }

    pub async fn delete(&self, user_id: Uuid) -> Result<u64, UserRepositoryError> {
        let result = sqlx::query!("DELETE FROM users WHERE id = $1", user_id)
            .execute(self.pools.select_pool(OperationType::Write))
            .await
            .map_err(UserRepositoryError::DatabaseError)?;

        Ok(result.rows_affected())
    }
}
