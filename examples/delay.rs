//! Scheduling delayed steps
use async_trait::async_trait;
use pg_task::{NextStep, Step, StepResult};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::Duration;

mod util;

// It wraps the task step into an enum which proxies necessary methods
pg_task::task!(Sleeper { Sleep, Wakeup });

// Also we need an enum representing all the possible tasks
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration as ChronoDuration, Utc};
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    fn lazy_pool() -> PgPool {
        PgPoolOptions::new()
            .connect_lazy("postgres:///pg_task")
            .unwrap()
    }

    #[tokio::test]
    async fn sleep_schedules_a_matching_wakeup() {
        let next = Sleep(3).step(&lazy_pool()).await.unwrap();

        match next {
            NextStep::Delayed(Sleeper::Wakeup(Wakeup(seconds)), delay) => {
                assert_eq!(seconds, 3);
                assert_eq!(delay, Duration::from_secs(3));
            }
            _ => panic!("expected the delayed wakeup step"),
        }
    }

    #[tokio::test]
    async fn sleep_allows_zero_delay_wakeups() {
        let next = Sleep(0).step(&lazy_pool()).await.unwrap();

        match next {
            NextStep::Delayed(Sleeper::Wakeup(Wakeup(seconds)), delay) => {
                assert_eq!(seconds, 0);
                assert_eq!(delay, Duration::ZERO);
            }
            _ => panic!("expected the delayed wakeup step"),
        }
    }

    #[tokio::test]
    async fn wakeup_finishes_the_task() {
        assert!(matches!(
            Wakeup(3).step(&lazy_pool()).await.unwrap(),
            NextStep::None
        ));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn worker_persists_delayed_wakeup_before_completing_it(pool: PgPool) {
        let started_at = Utc::now();
        let id = pg_task::enqueue(&pool, &Tasks::Sleeper(Sleep(1).into()))
            .await
            .unwrap();
        let worker = tokio::spawn({
            let pool = pool.clone();
            async move { pg_task::Worker::<Tasks>::new(pool).run().await }
        });

        let delayed_row = timeout(Duration::from_millis(500), async {
            loop {
                let row = sqlx::query!("SELECT step, wakeup_at FROM pg_task WHERE id = $1", id,)
                    .fetch_optional(&pool)
                    .await
                    .unwrap();
                if let Some(row) = row {
                    if row.step == serde_json::to_string(&Tasks::Sleeper(Wakeup(1).into())).unwrap()
                    {
                        return row;
                    }
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();

        assert!(delayed_row.wakeup_at >= started_at + ChronoDuration::seconds(1));

        assert!(timeout(Duration::from_millis(400), async {
            loop {
                if sqlx::query!("SELECT id FROM pg_task WHERE id = $1", id)
                    .fetch_optional(&pool)
                    .await
                    .unwrap()
                    .is_none()
                {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .is_err());

        timeout(Duration::from_secs(2), async {
            loop {
                if sqlx::query!("SELECT id FROM pg_task WHERE id = $1", id)
                    .fetch_optional(&pool)
                    .await
                    .unwrap()
                    .is_none()
                {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();

        sqlx::query!("NOTIFY pg_task_changed, 'stop_worker'")
            .execute(&pool)
            .await
            .unwrap();
        timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
