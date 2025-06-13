use sqlx::{PgPool, Row};

#[tokio::test]
async fn test_sqlx_postgres_connection() {
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:Francisco1@localhost:5432/banking_es".to_string()
    });

    let pool = PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to Postgres");

    let row = sqlx::query("SELECT 1 as one")
        .fetch_one(&pool)
        .await
        .expect("Failed to execute query");

    let value: i32 = row.get("one");
    assert_eq!(value, 1);
}
