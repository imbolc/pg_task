use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pg_fsm::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{env, time::Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let db = PgPool::connect(&env::var("DATABASE_URL")?).await?;

    // Let's schedule a couple few tasks
    Tasks::enqueue(&db, Start { up_to: 1 }).await?;

    // And run a worker
    pg_fsm::Worker::<Tasks>::new(db).run().await?;

    Ok(())
}

pg_fsm::scheduler!(Tasks { Count });

pg_fsm::task!(Count {
    Start,
    Proceed,
    Finish
});

#[derive(Debug, Deserialize, Serialize)]
pub struct Start {
    pub up_to: usize,
}
#[async_trait]
impl Step<Count> for Start {
    const RETRY_DELAY: Duration = Duration::from_secs(1);

    async fn step(self, _db: &PgPool) -> TaskResult<Option<Count>> {
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
    async fn step(self, _db: &PgPool) -> TaskResult<Option<Count>> {
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
    const RETRY: usize = 5;

    async fn step(self, _db: &PgPool) -> TaskResult<Option<Count>> {
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
