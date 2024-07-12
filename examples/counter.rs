//! A counter task gives some idea on the worker performatnce
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pg_task::{NextStep, Step, StepResult};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

mod util;

// It wraps the task step into an enum which proxies necessary methods
pg_task::task!(Count {
    Start,
    Proceed,
    Finish,
});

// Also we need a enum representing all the possible tasks
pg_task::scheduler!(Tasks { Count });

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = util::init().await?;

    // Let's schedule a few tasks
    pg_task::enqueue(&db, &Tasks::Count(Start { up_to: 1000 }.into())).await?;

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
    async fn step(self, _db: &PgPool) -> StepResult<Count> {
        println!("1..{}: start", self.up_to);
        NextStep::now(Proceed {
            up_to: self.up_to,
            started_at: Utc::now(),
            cur: 0,
        })
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
    async fn step(self, _db: &PgPool) -> StepResult<Count> {
        let Self {
            up_to,
            mut cur,
            started_at,
        } = self;

        cur += 1;
        if cur < up_to {
            NextStep::now(Proceed {
                up_to,
                started_at,
                cur,
            })
        } else {
            NextStep::now(Finish { up_to, started_at })
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
    async fn step(self, _db: &PgPool) -> StepResult<Count> {
        let took = Utc::now() - self.started_at;
        let secs = num_seconds(took);
        let per_sec = self.up_to as f64 / secs;
        println!(
            "1..{}: done in {} secs, {} / sec",
            self.up_to,
            secs,
            per_sec.round()
        );
        NextStep::none()
    }
}

fn num_seconds(duration: chrono::Duration) -> f64 {
    let seconds = duration.num_seconds();
    let nanos = duration.num_nanoseconds().unwrap() % 1_000_000_000;
    seconds as f64 + (nanos as f64 / 1_000_000_000.0)
}
