use crate::{Error, Result, Step, StepError};
use chrono::Utc;
use sqlx::{types::Uuid, PgPool};
use sqlx_error::sqlx_error;
use std::{marker::PhantomData, time::Duration};
use tracing::{debug, error};

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

impl<S: Step<S>> Worker<S> {
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
        sqlx::query!("UPDATE pg_task SET is_running = false")
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

        Ok(tasks)
    }

    /// Updates the tasks step
    pub async fn update_task(&self, id: Uuid, tasks: S) -> Result<()> {
        let step = serde_json::to_value(&tasks).map_err(Error::SerializeStep)?;
        sqlx::query!(
            "
            UPDATE pg_task
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
        err: StepError,
    ) -> Result<()> {
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
            "{task_id} scheduled {attempt} of {retry_limit} retry in {delay:?} on error: {}",
            source_chain::to_string(&*err),
            attempt = tried + 1
        );
        Ok(())
    }

    /// Sets the task error
    pub async fn set_task_error(&self, task_id: Uuid, tried: i32, err: StepError) -> Result<()> {
        let err = source_chain::to_string(&*err);
        sqlx::query!(
            "
            UPDATE pg_task
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
