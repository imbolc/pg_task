use async_trait::async_trait;
use pg_task::{NextStep, Step, StepResult};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Duration;

mod util;

// It wraps the task step into an enum which proxies necessary methods
pg_task::task!(Sleeper { Sleep, Wakeup });

// Also we need a enum representing all the possible tasks
pg_task::scheduler!(Tasks { Sleeper });

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = util::init().await?;

    // Let's schedule a few tasks
    for delay in [3, 1, 2] {
        pg_task::enqueue(&db, &Tasks::Sleeper(Sleep(delay).into())).await?;
    }

    // And run a worker
    pg_task::Worker::<Tasks>::new(db).run().await?;

    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Sleep(u64);
#[async_trait]
impl Step<Sleeper> for Sleep {
    async fn step(self, _db: &PgPool) -> StepResult<Sleeper> {
        println!("Sleeping for {} sec", self.0);
        NextStep::delay(Wakeup(self.0), Duration::from_secs(self.0))
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Wakeup(u64);
#[async_trait]
impl Step<Sleeper> for Wakeup {
    async fn step(self, _db: &PgPool) -> StepResult<Sleeper> {
        println!("Woke up after {} sec", self.0);
        NextStep::none()
    }
}
