use anyhow::Result;
use sqlx::PgPool;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

pub async fn init() -> Result<PgPool> {
    dotenv::dotenv().ok();
    init_logging()?;
    connect().await
}

async fn connect() -> Result<PgPool> {
    let db = sqlx::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    sqlx::migrate!().run(&db).await?;
    Ok(db)
}

fn init_logging() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

// Make `cargo check --examples` happy
#[allow(dead_code)]
fn main() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[sqlx::test(migrations = "./migrations")]
    async fn init_connects_and_runs_migrations(pool: PgPool) {
        let db_name: String = sqlx::query_scalar!(r#"SELECT current_database() AS "db_name!""#)
            .fetch_one(&pool)
            .await
            .unwrap();

        std::env::set_var("DATABASE_URL", format!("postgres:///{db_name}"));
        std::env::remove_var("RUST_LOG");

        let db = init().await.unwrap();
        let current_db: String = sqlx::query_scalar!(r#"SELECT current_database() AS "db_name!""#)
            .fetch_one(&db)
            .await
            .unwrap();

        assert_eq!(current_db, db_name);
        sqlx::query!("SELECT id FROM pg_task LIMIT 1")
            .fetch_optional(&db)
            .await
            .unwrap();
    }
}
