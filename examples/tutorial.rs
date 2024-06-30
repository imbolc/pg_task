use async_trait::async_trait;
use pg_task::{NextStep, Step, StepResult};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

mod util;

// Creates a enum `Greeter` containing our task steps
pg_task::task!(Greeter { ReadName, SayHello });

// Creates a enum `Tasks` representing all the possible tasks
pg_task::scheduler!(Tasks { Greeter });

#[derive(Debug, Deserialize, Serialize)]
pub struct ReadName {
    filename: String,
}
#[async_trait]
impl Step<Greeter> for ReadName {
    const RETRY_LIMIT: i32 = 5;

    async fn step(self, _db: &PgPool) -> StepResult<Greeter> {
        let name = std::fs::read_to_string(self.filename)?;
        NextStep::now(SayHello { name })
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SayHello {
    name: String,
}
#[async_trait]
impl Step<Greeter> for SayHello {
    async fn step(self, _db: &PgPool) -> StepResult<Greeter> {
        println!("Hello, {}", self.name);
        NextStep::none()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = util::init().await?;

    // Let's schedule the task
    pg_task::enqueue(
        &db,
        &Tasks::Greeter(
            ReadName {
                filename: "name.txt".into(),
            }
            .into(),
        ),
    )
    .await?;

    // And run a worker
    pg_task::Worker::<Tasks>::new(db).run().await?;

    Ok(())
}
