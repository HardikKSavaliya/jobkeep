//! Test databases: each test gets a fresh schema in the database named by
//! `JOBKEEP_TEST_DATABASE_URL`.
//!
//! Without the variable the tests are skipped (they print a note and pass),
//! except on CI, where a missing database is an error.

use std::str::FromStr;

use jobkeep_postgres::PgStorage;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, Connection, PgConnection};

pub const URL_VAR: &str = "JOBKEEP_TEST_DATABASE_URL";

pub async fn storage() -> Option<PgStorage> {
    let Ok(url) = std::env::var(URL_VAR) else {
        assert!(
            std::env::var_os("CI").is_none(),
            "{URL_VAR} must be set on CI"
        );
        eprintln!("skipping Postgres test: {URL_VAR} is not set");
        return None;
    };

    let schema = format!("jobkeep_test_{}", uuid::Uuid::new_v4().simple());
    let mut admin = PgConnection::connect(&url)
        .await
        .expect("connect to test database");
    // The name is generated above, never user input.
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&mut admin)
        .await
        .expect("create test schema");
    admin.close().await.ok();

    let options = PgConnectOptions::from_str(&url)
        .expect("valid database url")
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .expect("connect test pool");
    let storage = PgStorage::new(pool);
    storage.migrate().await.expect("run migrations");
    Some(storage)
}
