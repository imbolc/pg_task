//! # pg-fsm
//!
//! Resumable FSM-based Postgres tasks
#![warn(clippy::all, missing_docs, nonstandard_style, future_incompatible)]

use async_trait::async_trait;
use chrono::Utc;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{types::Uuid, PgPool};
use sqlx_error::sqlx_error;
use std::{
    error::Error as StdError, fmt, marker::PhantomData, result::Result as StdResult, time::Duration,
};

/// The crate error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Sqlx error with some context
    #[error(transparent)]
    Sqlx(#[from] sqlx_error::SqlxError),
    /// Task serialization issue - shouldn't ever happen
    #[error("serialize task")]
    SerializeTask(#[source] serde_json::Error),
    /// Task deserialization issue - could happen if task changed while the table isn't empty
    #[error("deserialize task")]
    DeserializeTask(#[source] serde_json::Error),
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
    /// How many times retry the step on an error
    const RETRY: usize = 0;

    /// The time to wait between retries
    const RETRY_DELAY: Duration = Duration::from_secs(0);

    /// Processes the current step and returns the next if any
    async fn step(self, db: &PgPool) -> TaskResult<Option<Task>>;

    /// Proxies the `RETRY` const, doesn't mean to be changed in impls
    fn retry(&self) -> usize {
        Self::RETRY
    }

    /// Proxies the `RETRY_DELAY` const, doesn't mean to be changed in impls
    fn retry_delay(&self) -> Duration {
        Self::RETRY_DELAY
    }
}

/// A tait to implement on the outer enum wrapper containing all the tasks
#[async_trait]
pub trait TaskSet
where
    Self: fmt::Debug + DeserializeOwned + Serialize + Sized,
{
    /// Adds a new task to the queue
    async fn run<S: Step<T>, T: Into<Self> + Send>(db: &PgPool, step: S) -> Result<Uuid> {
        dbg!(&step, step.retry(), step.retry_delay());
        let task = step.into();
        let tasks: Self = task.into();
        let step = serde_json::to_value(&tasks).map_err(Error::SerializeTask)?;
        let now = Utc::now();
        sqlx::query!(
            "
            INSERT INTO fsm (id, tried, is_running, step,  wakeup_at, created_at, updated_at)
            VALUES (gen_random_uuid(),    0,      false,   $1,         $2,         $2,         $2)
            RETURNING id
            ",
            step,
            now,
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

impl<T: TaskSet + Step<T>> Worker<T> {
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
            for (id, task) in tasks {
                self.run_task_step(id, task).await?;
            }
        }
    }

    /// Runs a single step of the first task in the queue
    async fn run_task_step(&self, task_id: Uuid, task: T) -> Result<()> {
        dbg!(task_id, &task, task.retry(), task.retry_delay());
        match task.step(&self.db).await {
            Err(e) => self.set_task_error(task_id, e).await?,
            Ok(None) => self.finish_task(task_id).await?,
            Ok(Some(step)) => self.update_task(task_id, step).await?,
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

    /// Locks the next task as running and returns it
    async fn lock_and_fetch_tasks(&self, limit: i64) -> Result<Vec<(Uuid, T)>> {
        let mut tx = self.db.begin().await.map_err(sqlx_error!("begin"))?;

        let rows = sqlx::query!(
            "
            SELECT
                id,
                -- tried,
                step
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
            let task: T = serde_json::from_value(row.step).map_err(Error::DeserializeTask)?;
            // let retry = row.tried < task.RETRY;
            tasks.push((row.id, task));
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
    pub async fn update_task(&self, id: Uuid, tasks: T) -> Result<()> {
        let step = serde_json::to_value(&tasks).map_err(Error::SerializeTask)?;
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

    /// Sets the task error
    pub async fn set_task_error(&self, task_id: Uuid, err: TaskError) -> Result<()> {
        let err_str = source_chain::to_string(&*err);
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
            err_str,
            Utc::now(),
        )
        .execute(&self.db)
        .await
        .map_err(sqlx_error!())?;
        Ok(())
    }
}

#[macro_export]
/// Implements enum wrapper for a single task containing all it's steps
macro_rules! pg_task {
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
        impl Step<$enum> for $enum {
            async fn step(self, db: &sqlx::PgPool) -> TaskResult<Option<$enum>> {
                Ok(match self {
                    $(Self::$variant(inner) => inner.step(db).await?.map(Into::into),)*
                })
            }

            fn retry(&self) -> usize {
                match self {
                    $(Self::$variant(inner) => inner.retry(),)*
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
macro_rules! pg_task_runner {
    ($enum:ident { $($variant:ident),* }) => {
        $crate::pg_task!($enum { $($variant),* });

        #[async_trait]
        impl TaskSet for $enum {}
    }
}
