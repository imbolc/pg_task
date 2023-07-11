use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pg_task::{Step, StepResult};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{env, time::Duration};

// It wraps the task step into an enum which proxies necessary methods
pg_task::task!(Count {
    Start,
    Proceed,
    Finish
});

// Also we need a enum representing all the possible tasks
pg_task::scheduler!(Tasks { Count });

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = connect().await?;
    init_logging()?;

    // Let's schedule a few tasks
    Tasks::Count(Start { up_to: 2 }.into()).enqueue(&db).await?;

    // And run a worker
    pg_task::Worker::<Tasks>::new(db).run().await?;

    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Start {
    pub up_to: usize,
}
#[async_trait]
impl Step<Count> for Start {
    async fn step(self, _db: &PgPool) -> StepResult<Option<Count>> {
        println!("1..{}: start", self.up_to);
        Ok(Some(
            Proceed {
                up_to: self.up_to,
                started_at: Utc::now(),
                cur: 0,
            }
            .into(),
        ))
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Proceed {
    pub up_to: usize,
    pub started_at: DateTime<Utc>,
    pub cur: usize,
}
#[async_trait]
impl Step<Count> for Proceed {
    const RETRY_LIMIT: i32 = 5;
    const RETRY_DELAY: Duration = Duration::from_secs(1);

    async fn step(self, _db: &PgPool) -> StepResult<Option<Count>> {
        // return Err(anyhow::anyhow!("bailing").into());
        let Self {
            up_to,
            mut cur,
            started_at,
        } = self;
        cur += 1;
        // println!("1..{up_to}: {cur}");
        if cur < up_to {
            Ok(Some(
                Proceed {
                    up_to,
                    started_at,
                    cur,
                }
                .into(),
            ))
        } else {
            Ok(Some(Finish { up_to, started_at }.into()))
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Finish {
    pub up_to: usize,
    pub started_at: DateTime<Utc>,
}
#[async_trait]
impl Step<Count> for Finish {
    async fn step(self, _db: &PgPool) -> StepResult<Option<Count>> {
        let took = Utc::now() - self.started_at;
        let secs = num_seconds(took);
        let per_sec = self.up_to as f64 / secs;
        println!(
            "1..{}: done in {} secs, {} / sec",
            self.up_to,
            secs,
            per_sec.round()
        );
        Ok(None)
    }
}

fn num_seconds(duration: chrono::Duration) -> f64 {
    let seconds = duration.num_seconds();
    let nanos = duration.num_nanoseconds().unwrap() % 1_000_000_000;
    seconds as f64 + (nanos as f64 / 1_000_000_000.0)
}

async fn connect() -> anyhow::Result<sqlx::PgPool> {
    dotenv::dotenv().ok();
    let db = PgPool::connect(&env::var("DATABASE_URL")?).await?;
    sqlx::migrate!().run(&db).await?;
    Ok(db)
}

fn init_logging() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}
