//! # pg-fsm
//!
//! Resumable FSM-based Postgres tasks
//!
//!
//! # TODO
//! - [x] retry
//! - [ ] counurrency
//! - [ ] logging
//! - [ ] docs
//! - [ ] sqlx v7
//!
#![warn(clippy::all, missing_docs, nonstandard_style, future_incompatible)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{types::Uuid, PgPool};
use sqlx_error::sqlx_error;
use std::{
    error::Error as StdError, fmt, marker::PhantomData, result::Result as StdResult, time::Duration,
};
use tracing::{debug, error};

pub mod prelude;

/// The crate error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Sqlx error with some context
    #[error(transparent)]
    Sqlx(#[from] sqlx_error::SqlxError),
    /// Task serialization issue - shouldn't ever happen
    #[error("serialize step")]
    SerializeStep(#[source] serde_json::Error),
    /// Task deserialization issue - could happen if task changed while the table isn't empty
    #[error("deserialize step")]
    DeserializeStep(#[source] serde_json::Error),
}

type Result<T> = StdResult<T, Error>;

/// Error for tasks
pub type TaskError = Box<dyn StdError + 'static>;
/// Result for tasks
pub type TaskResult<T> = StdResult<T, TaskError>;

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
    async fn step(self, db: &PgPool) -> TaskResult<Option<Task>>;

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

/// A worker too process tasks
pub struct Worker<T> {
    db: PgPool,
    tasks: PhantomData<T>,
}

/// Task step with some db fields
#[derive(Debug)]
struct Task<S> {
    id: Uuid,
    step: S,
    tried: i32,
}

impl<S: Scheduler + Step<S>> Worker<S> {
    /// Creates a new worker
    pub fn new(db: PgPool) -> Self {
        Self {
            db,
            tasks: PhantomData,
        }
    }

    /// Runs all the tasks to completion
    pub async fn run(&self) -> Result<()> {
        self.unlock_all_tasks().await?;
        loop {
            let tasks = self.lock_and_fetch_tasks(100).await?;
            if tasks.is_empty() {
                // TODO subscribe to the table changes
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            // TODO parallelization
            for task in tasks {
                self.run_step(task).await?;
            }
        }
    }

    /// Runs a single step of the first task in the queue
    async fn run_step(&self, task: Task<S>) -> Result<()> {
        let Task { id, step, tried } = task;
        let retry_limit = step.retry_limit();
        let retry_delay = step.retry_delay();
        match step.step(&self.db).await {
            Err(e) => {
                self.process_error(id, tried, retry_limit, retry_delay, e)
                    .await?
            }
            Ok(None) => self.finish_task(id).await?,
            Ok(Some(step)) => self.update_task(id, step).await?,
        };
        Ok(())
    }

    /// Unlocks all tasks. It meant to run at the worker start as some tasks could stuck locked forever
    /// if the previous run ended up with some kind of a crush.
    pub async fn unlock_all_tasks(&self) -> Result<()> {
        sqlx::query!("UPDATE fsm SET is_running = false")
            .execute(&self.db)
            .await
            .map_err(sqlx_error!())?;
        Ok(())
    }

    /// Locks next task as running and returns it
    async fn lock_and_fetch_tasks(&self, limit: i64) -> Result<Vec<Task<S>>> {
        let mut tx = self.db.begin().await.map_err(sqlx_error!("begin"))?;

        let rows = sqlx::query!(
            "
            SELECT
                id,
                step,
                tried
            FROM fsm
            WHERE wakeup_at <= now()
              AND is_running = false
              AND error IS NULL
            ORDER BY wakeup_at
            LIMIT $1
            FOR UPDATE
            ",
            limit
        )
        .fetch_all(&mut tx)
        .await
        .map_err(sqlx_error!("select"))?;

        let mut ids = Vec::with_capacity(rows.len());
        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            ids.push(row.id);
            let step: S = serde_json::from_value(row.step).map_err(Error::DeserializeStep)?;
            tasks.push(Task {
                id: row.id,
                step,
                tried: row.tried,
            });
        }

        sqlx::query!(
            "
            UPDATE fsm
            SET is_running = true,
                updated_at = now()
            WHERE id = any($1)
            ",
            &ids
        )
        .execute(&mut tx)
        .await
        .map_err(sqlx_error!("lock"))?;
        tx.commit().await.map_err(sqlx_error!("commit"))?;

        Ok(tasks)
    }

    /// Updates the tasks step
    pub async fn update_task(&self, id: Uuid, tasks: S) -> Result<()> {
        let step = serde_json::to_value(&tasks).map_err(Error::SerializeStep)?;
        sqlx::query!(
            "
            UPDATE fsm
            SET is_running = false,
                step = $2,
                updated_at = $3,
                wakeup_at = $3
            WHERE id = $1
            ",
            id,
            step,
            Utc::now(),
        )
        .execute(&self.db)
        .await
        .map_err(sqlx_error!())?;
        Ok(())
    }

    /// Removes a finished task
    pub async fn finish_task(&self, task_id: Uuid) -> Result<()> {
        sqlx::query!("DELETE FROM fsm WHERE id = $1", task_id)
            .execute(&self.db)
            .await
            .map_err(sqlx_error!())?;
        Ok(())
    }

    /// Dealing with the step error
    async fn process_error(
        &self,
        task_id: Uuid,
        tried: i32,
        retry_limit: i32,
        retry_delay: Duration,
        err: TaskError,
    ) -> Result<()> {
        if tried < retry_limit {
            self.retry_task(task_id, tried, retry_limit, retry_delay, err)
                .await
        } else {
            self.set_task_error(task_id, tried, err).await
        }
    }

    /// Schedules the task retry
    pub async fn retry_task(
        &self,
        task_id: Uuid,
        tried: i32,
        retry_limit: i32,
        delay: Duration,
        err: TaskError,
    ) -> Result<()> {
        let wakeup_at = Utc::now() + chrono_duration(delay);
        sqlx::query!(
            "
            UPDATE fsm
            SET is_running = false,
                tried = tried + 1,
                updated_at = now(),
                wakeup_at = $2
            WHERE id = $1
            ",
            task_id,
            wakeup_at,
        )
        .execute(&self.db)
        .await
        .map_err(sqlx_error!())?;

        debug!(
            "{task_id} scheduled {attempt} of {retry_limit} retry in {delay:?} on error: {}",
            source_chain::to_string(&*err),
            attempt = tried + 1
        );
        Ok(())
    }

    /// Sets the task error
    pub async fn set_task_error(&self, task_id: Uuid, tried: i32, err: TaskError) -> Result<()> {
        let err = source_chain::to_string(&*err);
        sqlx::query!(
            "
            UPDATE fsm
            SET is_running = false,
                error = $2,
                updated_at = $3,
                wakeup_at = $3
            WHERE id = $1
            ",
            task_id,
            &err,
            Utc::now(),
        )
        .execute(&self.db)
        .await
        .map_err(sqlx_error!())?;
        error!(
            "{task_id} resulted in an error after {tried} attempts: {}",
            &err
        );
        Ok(())
    }
}

/// Implements enum wrapper for a single task containing all it's steps
#[macro_export]
macro_rules! task {
    ($enum:ident { $($variant:ident),* }) => {
        #[derive(Debug, serde::Deserialize, serde::Serialize)]
        pub enum $enum {
            $($variant($variant),)*
        }

        $(
            impl From<$variant> for $enum {
                fn from(inner: $variant) -> Self {
                    Self::$variant(inner)
                }
            }
        )*

        #[async_trait::async_trait]
        impl $crate::Step<$enum> for $enum {
            async fn step(self, db: &sqlx::PgPool) -> $crate::TaskResult<Option<$enum>> {
                Ok(match self {
                    $(Self::$variant(inner) => inner.step(db).await?.map(Into::into),)*
                })
            }

            fn retry_limit(&self) -> i32 {
                match self {
                    $(Self::$variant(inner) => inner.retry_limit(),)*
                }
            }

            fn retry_delay(&self) -> std::time::Duration {
                match self {
                    $(Self::$variant(inner) => inner.retry_delay(),)*
                }
            }
        }
    }
}

/// The macro implements the outer enum wrapper containing all the tasks
#[macro_export]
macro_rules! scheduler {
    ($enum:ident { $($variant:ident),* }) => {
        $crate::task!($enum { $($variant),* });

        #[async_trait]
        impl $crate::Scheduler for $enum {}
    }
}

fn chrono_duration(std_duration: Duration) -> chrono::Duration {
    chrono::Duration::from_std(std_duration).unwrap_or_else(|_| chrono::Duration::max_value())
}
