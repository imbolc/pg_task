//! A counter task gives some idea on the worker performance
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

// Also we need an enum representing all the possible tasks
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

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    fn lazy_pool() -> PgPool {
        PgPoolOptions::new()
            .connect_lazy("postgres:///pg_task")
            .unwrap()
    }

    #[tokio::test]
    async fn start_advances_to_the_first_proceed_step() {
        let next = Start { up_to: 3 }.step(&lazy_pool()).await.unwrap();

        match next {
            NextStep::Now(Count::Proceed(Proceed {
                up_to,
                started_at: _,
                cur,
            })) => {
                assert_eq!(up_to, 3);
                assert_eq!(cur, 0);
            }
            _ => panic!("expected the first proceed step"),
        }
    }

    #[tokio::test]
    async fn proceed_keeps_counting_before_the_limit() {
        let started_at = Utc::now();
        let next = Proceed {
            up_to: 3,
            started_at,
            cur: 1,
        }
        .step(&lazy_pool())
        .await
        .unwrap();

        match next {
            NextStep::Now(Count::Proceed(Proceed {
                up_to,
                started_at: next_started_at,
                cur,
            })) => {
                assert_eq!(up_to, 3);
                assert_eq!(cur, 2);
                assert_eq!(next_started_at, started_at);
            }
            _ => panic!("expected the next proceed step"),
        }
    }

    #[tokio::test]
    async fn proceed_finishes_when_the_limit_is_reached() {
        let started_at = Utc::now();
        let next = Proceed {
            up_to: 3,
            started_at,
            cur: 2,
        }
        .step(&lazy_pool())
        .await
        .unwrap();

        match next {
            NextStep::Now(Count::Finish(Finish {
                up_to,
                started_at: finished_started_at,
            })) => {
                assert_eq!(up_to, 3);
                assert_eq!(finished_started_at, started_at);
            }
            _ => panic!("expected the finish step"),
        }
    }

    #[tokio::test]
    async fn proceed_finishes_single_step_counts() {
        let started_at = Utc::now();
        let next = Proceed {
            up_to: 1,
            started_at,
            cur: 0,
        }
        .step(&lazy_pool())
        .await
        .unwrap();

        match next {
            NextStep::Now(Count::Finish(Finish {
                up_to,
                started_at: finished_started_at,
            })) => {
                assert_eq!(up_to, 1);
                assert_eq!(finished_started_at, started_at);
            }
            _ => panic!("expected the finish step"),
        }
    }

    #[tokio::test]
    async fn finish_completes_the_task() {
        assert!(matches!(
            Finish {
                up_to: 3,
                started_at: Utc::now()
            }
            .step(&lazy_pool())
            .await
            .unwrap(),
            NextStep::None
        ));
    }

    #[test]
    fn num_seconds_preserves_fractional_precision() {
        let duration = chrono::Duration::seconds(2) + chrono::Duration::milliseconds(250);

        assert!((num_seconds(duration) - 2.25).abs() < f64::EPSILON);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn worker_counts_to_completion(pool: PgPool) {
        let id = pg_task::enqueue(&pool, &Tasks::Count(Start { up_to: 3 }.into()))
            .await
            .unwrap();
        let worker = tokio::spawn({
            let pool = pool.clone();
            async move { pg_task::Worker::<Tasks>::new(pool).run().await }
        });

        timeout(Duration::from_secs(1), async {
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
