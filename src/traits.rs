use crate::{Error, StepResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{types::Uuid, PgPool};
use std::{fmt, time::Duration};

/// A tait to implement on each task step
#[async_trait]
pub trait Step<Task>
where
    Task: Sized,
    Self: Into<Task> + Send + Sized + fmt::Debug + DeserializeOwned + Serialize,
{
    /// How many times retry_limit the step on an error
    const RETRY_LIMIT: i32 = 0;

    /// The time to wait between retries
    const RETRY_DELAY: Duration = Duration::from_secs(1);

    /// Processes the current step and returns the next if any
    async fn step(self, db: &PgPool) -> StepResult<Task>;

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
pub trait Scheduler: fmt::Debug + DeserializeOwned + Serialize + Sized + Sync {
    /// Enqueues the task to be run immediately
    async fn enqueue(&self, db: &PgPool) -> crate::Result<Uuid> {
        self.schedule(db, Utc::now()).await
    }

    /// Schedules a task to be run after a specified delay
    async fn delay(&self, db: &PgPool, delay: Duration) -> crate::Result<Uuid> {
        let delay =
            chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::max_value());
        self.schedule(db, Utc::now() + delay).await
    }

    /// Schedules a task to run at a specified time in the future
    async fn schedule(&self, db: &PgPool, at: DateTime<Utc>) -> crate::Result<Uuid> {
        let step = serde_json::to_string(self).map_err(Error::SerializeStep)?;
        sqlx::query!(
            "INSERT INTO pg_task (step, wakeup_at) VALUES ($1, $2) RETURNING id",
            step,
            at
        )
        .map(|r| r.id)
        .fetch_one(db)
        .await
        .map_err(Error::AddTask)
    }
}
