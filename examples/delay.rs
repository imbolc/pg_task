use async_trait::async_trait;
use pg_task::{NextStep, Step, StepResult};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{env, time::Duration};

// It wraps the task step into an enum which proxies necessary methods
pg_task::task!(FooBar { Foo, Bar });

// Also we need a enum representing all the possible tasks
pg_task::scheduler!(Tasks { FooBar });

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = connect().await?;

    // Let's schedule a few tasks
    for delay in [3, 1, 2] {
        pg_task::enqueue(&db, &Tasks::FooBar(Foo(delay).into())).await?;
    }

    // And run a worker
    pg_task::Worker::<Tasks>::new(db).run().await;

    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Foo(u64);
#[async_trait]
impl Step<FooBar> for Foo {
    async fn step(self, _db: &PgPool) -> StepResult<FooBar> {
        println!("Sleeping for {} sec", self.0);
        NextStep::delay(Bar(self.0), Duration::from_secs(self.0))
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Bar(u64);
#[async_trait]
impl Step<FooBar> for Bar {
    async fn step(self, _db: &PgPool) -> StepResult<FooBar> {
        println!("Woke up after {} sec", self.0);
        NextStep::none()
    }
}

async fn connect() -> anyhow::Result<sqlx::PgPool> {
    dotenv::dotenv().ok();
    let db = PgPool::connect(&env::var("DATABASE_URL")?).await?;
    sqlx::migrate!().run(&db).await?;
    Ok(db)
}
