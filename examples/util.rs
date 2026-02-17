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
