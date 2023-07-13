use crate::{Step, StepError};
use chrono::Utc;
use code_path::code_path;
use sqlx::{types::Uuid, PgPool};
use std::{marker::PhantomData, time::Duration};
use tokio::time::sleep;
use tracing::{debug, error, trace};

/// A private error to log from worker
#[derive(Debug, thiserror::Error)]
enum ErrorReport {
    /// Sqlx error with some context
    #[error("sqlx: {1}")]
    Sqlx(#[source] sqlx::Error, String),
    /// Task serialization issue - shouldn't ever happen
    #[error("serialize step: {1}")]
    SerializeStep(#[source] serde_json::Error, String),
    /// Task deserialization issue - could happen if task changed while the table isn't empty
    #[error("can't deserialize step `{1}` (tasks code has probably been changed after scheduling this step)")]
    DeserializeStep(#[source] serde_json::Error, String),
}

type Result<T> = std::result::Result<T, ErrorReport>;

macro_rules! sqlx_error {
    () => {
        |e| ErrorReport::Sqlx(e, code_path!().into())
    };
    ($desc:expr) => {
        |e| ErrorReport::Sqlx(e, format!("{} {}", code_path!(), $desc))
    };
}

/// A worker to process tasks
pub struct Worker<T> {
    db: PgPool,
    tasks: PhantomData<T>,
}

#[derive(Debug)]
struct Task<S> {
    id: Uuid,
    step: S,
    tried: i32,
}

impl ErrorReport {
    fn log(self) {
        error!("{}", source_chain::to_string(&self));
    }
}

impl<S: Step<S>> Worker<S> {
    /// Creates a new worker
    pub fn new(db: PgPool) -> Self {
        Self {
            db,
            tasks: PhantomData,
        }
    }

    /// Runs all the tasks to completion, and watches for changes
    pub async fn run(&self) {
        self.unlock_stale_tasks()
            .await
            .map_err(ErrorReport::log)
            .ok();

        loop {
            let Ok(tasks) = self.fetch_ready_tasks(100).await.map_err(ErrorReport::log)
            else {
                sleep(Duration::from_secs(1)).await;
                continue;
            };

            if tasks.is_empty() {
                // TODO subscribe to the table changes
                sleep(Duration::from_secs(1)).await;
            }

            // TODO concurrency
            for task in tasks {
                self.run_step(task).await.map_err(ErrorReport::log).ok();
            }
        }
    }

    /// Runs a single step of the first task in the queue
    // #[tracing::instrument(skip(self))]
    async fn run_step(&self, task: Task<S>) -> Result<()> {
        let Task { id, step, tried } = task;
        let retry_limit = step.retry_limit();
        let retry_delay = step.retry_delay();
        debug!(
            "[{id}] {attempt} of {max_attempts} attempt to run step {step:?}",
            attempt = tried + 1,
            max_attempts = retry_limit + 1
        );
        match step.step(&self.db).await {
            Err(e) => {
                self.process_error(id, tried, retry_limit, retry_delay, e)
                    .await?
            }
            Ok(None) => self.finish_task(id).await?,
            Ok(Some(step)) => self.update_task_step(id, step).await?,
        };
        Ok(())
    }

    /// Unlocks all tasks. It meant to run at the worker start as some tasks could stuck locked forever
    /// if the previous run ended up with some kind of a crush.
    async fn unlock_stale_tasks(&self) -> Result<()> {
        let unlocked =
            sqlx::query!("UPDATE pg_task SET is_running = false WHERE is_running = true")
                .execute(&self.db)
                .await
                .map_err(sqlx_error!())?
                .rows_affected();
        if unlocked == 0 {
            debug!("No stale tasks to unlock")
        } else {
            debug!("Unlocked {} stale tasks", unlocked)
        }
        Ok(())
    }

    /// Locks next task as running and returns it
    async fn fetch_ready_tasks(&self, limit: i64) -> Result<Vec<Task<S>>> {
        trace!("Fetching ready to run tasks");

        let mut tx = self.db.begin().await.map_err(sqlx_error!("begin"))?;

        let rows = sqlx::query!(
            "
            SELECT
                id,
                step,
                tried
            FROM pg_task
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

        let ids: Vec<_> = rows.iter().map(|r| r.id).collect();
        sqlx::query!(
            "
            UPDATE pg_task
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

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let step: S = match serde_json::from_value(row.step)
                .map_err(|e| ErrorReport::DeserializeStep(e, format!("{:?}", tasks)))
            {
                Ok(x) => x,
                Err(e) => {
                    self.set_task_error(row.id, e.into())
                        .await
                        .map_err(ErrorReport::log)
                        .ok();
                    continue;
                }
            };

            tasks.push(Task {
                id: row.id,
                step,
                tried: row.tried,
            });
        }

        if tasks.is_empty() {
            trace!("No ready to run tasks found");
        } else {
            debug!("Found tasks ready to run: {}", tasks.len());
        }
        Ok(tasks)
    }

    /// Updates the tasks step
    async fn update_task_step(&self, task_id: Uuid, step: S) -> Result<()> {
        trace!("[{task_id}] update step to {step:?}");

        let step = match serde_json::to_value(&step)
            .map_err(|e| ErrorReport::SerializeStep(e, format!("{:?}", step)))
        {
            Ok(x) => x,
            Err(e) => {
                self.set_task_error(task_id, e.into())
                    .await
                    .map_err(ErrorReport::log)
                    .ok();
                return Ok(());
            }
        };

        sqlx::query!(
            "
            UPDATE pg_task
            SET is_running = false,
                tried = 0,
                step = $2,
                updated_at = $3,
                wakeup_at = $3
            WHERE id = $1
            ",
            task_id,
            step,
            Utc::now(),
        )
        .execute(&self.db)
        .await
        .map_err(sqlx_error!())?;

        debug!("[{task_id}] step is done");
        Ok(())
    }

    /// Removes the finished task
    async fn finish_task(&self, task_id: Uuid) -> Result<()> {
        debug!("[{task_id}] is successfully completed");
        sqlx::query!("DELETE FROM pg_task WHERE id = $1", task_id)
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
        err: StepError,
    ) -> Result<()> {
        if tried < retry_limit {
            self.retry_task(task_id, tried, retry_limit, retry_delay, err)
                .await
        } else {
            self.set_task_error(task_id, err).await
        }
    }

    /// Schedules the task retry
    async fn retry_task(
        &self,
        task_id: Uuid,
        tried: i32,
        retry_limit: i32,
        delay: Duration,
        err: StepError,
    ) -> Result<()> {
        trace!("[{task_id}] scheduling retry");

        let delay =
            chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::max_value());
        let wakeup_at = Utc::now() + delay;
        sqlx::query!(
            "
            UPDATE pg_task
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
            "[{task_id}] scheduled {attempt} of {retry_limit} retry in {delay:?} on error: {}",
            source_chain::to_string(&*err),
            attempt = tried + 1
        );
        Ok(())
    }

    /// Sets the task error
    async fn set_task_error(&self, task_id: Uuid, err: StepError) -> Result<()> {
        trace!("[{task_id}] saving error");

        let err = source_chain::to_string(&*err);
        let (tried, step) = sqlx::query!(
            r#"
            UPDATE pg_task
            SET is_running = false,
                error = $2,
                updated_at = $3,
                wakeup_at = $3
            WHERE id = $1
            RETURNING tried, step::TEXT as "step!"
            "#,
            task_id,
            &err,
            Utc::now(),
        )
        .fetch_one(&self.db)
        .await
        .map(|r| (r.tried, r.step))
        .map_err(sqlx_error!())?;
        error!(
            "[{task_id}] resulted in an error on step {step} after {tried} attempts: {}",
            &err
        );
        Ok(())
    }
}
