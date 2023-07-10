use crate::{utils::chrono_duration, Error, Result, StepResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{types::Uuid, PgPool};
use sqlx_error::sqlx_error;
use std::{fmt, time::Duration};

/// A tait to implement on each task step
#[async_trait]
pub trait Step<Task>
where
    Task: Sized,
    Self: Into<Task> + Send + Sized + fmt::Debug,
{
    /// How many times retry_limit the step on an error
    const RETRY_LIMIT: i32 = 0;

    /// The time to wait between retries
    const RETRY_DELAY: Duration = Duration::ZERO;

    /// Processes the current step and returns the next if any
    async fn step(self, db: &PgPool) -> StepResult<Option<Task>>;

    /// Proxies the `RETRY` const, doesn't mean to be changed in impls
    fn retry_limit(&self) -> i32 {
        Self::RETRY_LIMIT
    }

    /// Proxies the `RETRY_DELAY` const, doesn't mean to be changed in impls
    fn retry_delay(&self) -> Duration {
        Self::RETRY_DELAY
    }
}

/// A tait to implement on the outer enum wrapper containing all the tasks
#[async_trait]
pub trait Scheduler
where
    Self: fmt::Debug + DeserializeOwned + Serialize + Sized,
{
    /// Enqueues the task to be run immediately
    async fn enqueue<S: Step<T>, T: Into<Self> + Send>(db: &PgPool, step: S) -> Result<Uuid> {
        Self::schedule(db, step, Utc::now()).await
    }

    /// Schedules a task to be run after a specified delay
    async fn delay<S: Step<T>, T: Into<Self> + Send>(
        db: &PgPool,
        step: S,
        delay: Duration,
    ) -> Result<Uuid> {
        let delay = chrono_duration(delay);
        Self::schedule(db, step, Utc::now() + delay).await
    }

    /// Schedules a task to run at a specified time in the future
    async fn schedule<S: Step<T>, T: Into<Self> + Send>(
        db: &PgPool,
        step: S,
        at: DateTime<Utc>,
    ) -> Result<Uuid> {
        let task = step.into();
        let tasks: Self = task.into();
        let step = serde_json::to_value(&tasks).map_err(Error::SerializeStep)?;
        sqlx::query!(
            "INSERT INTO fsm (step, wakeup_at) VALUES ($1, $2) RETURNING id",
            step,
            at
        )
        .map(|r| r.id)
        .fetch_one(db)
        .await
        .map_err(sqlx_error!(format!("add task {:?}", tasks)))
        .map_err(Into::into)
    }
}
