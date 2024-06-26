use crate::{util, waiter::Waiter, NextStep, Step, StepError};
use chrono::{DateTime, Utc};
use code_path::code_path;
use sqlx::{
    postgres::{PgConnection, PgPool},
    types::Uuid,
};
use std::{marker::PhantomData, time::Duration};
use tokio::time::sleep;
use tracing::{debug, error, info, trace};

/// An error report to log from the worker
#[derive(Debug, displaydoc::Display, thiserror::Error)]
enum ErrorReport {
    /// db error: {1}
    Sqlx(#[source] sqlx::Error, String),
    /// can't serialize step: {1}
    SerializeStep(#[source] serde_json::Error, String),
    /**
    can't deserialize step (the task was likely changed between the
    scheduling and running of the step): {1}
    */
    DeserializeStep(#[source] serde_json::Error, String),
}

type ReportResult<T> = std::result::Result<T, ErrorReport>;

macro_rules! sqlx_error {
    () => {
        |e| ErrorReport::Sqlx(e, code_path!().into())
    };
    ($desc:expr) => {
        |e| ErrorReport::Sqlx(e, format!("{} {}", code_path!(), $desc))
    };
}

/// A worker for processing tasks
pub struct Worker<T> {
    db: PgPool,
    waiter: Waiter,
    tasks: PhantomData<T>,
}

#[derive(Debug)]
struct Task {
    id: Uuid,
    step: String,
    tried: i32,
    wakeup_at: DateTime<Utc>,
}

impl ErrorReport {
    fn log(self) {
        error!("{}", source_chain::to_string(&self));
    }
}

impl Task {
    /// Returns the delay time before running the task
    fn delay(&self) -> Duration {
        let delay = self.wakeup_at - Utc::now();
        if delay <= chrono::Duration::zero() {
            Duration::ZERO
        } else {
            util::chrono_duration_to_std(delay)
        }
    }
}

impl<S: Step<S>> Worker<S> {
    /// Creates a new worker
    pub fn new(db: PgPool) -> Self {
        let waiter = Waiter::new();
        Self {
            db,
            waiter,
            tasks: PhantomData,
        }
    }

    /// Runs all ready tasks to completion and waits for new ones
    pub async fn run(&self) -> crate::Result<()> {
        self.unlock_stale_tasks().await?;
        self.waiter.listen(self.db.clone()).await?;

        // TODO concurrency
        loop {
            let Ok(task) = self.recv_task().await.map_err(ErrorReport::log) else {
                sleep(Duration::from_secs(1)).await;
                continue;
            };
            self.run_step(task).await.map_err(ErrorReport::log).ok();
        }
    }

    /// Runs the next step of the task
    async fn run_step(&self, task: Task) -> ReportResult<()> {
        let Task {
            id, step, tried, ..
        } = task;
        info!(
            "[{id}]{} run step {step}",
            if tried > 0 {
                format!(" {} attempt to", util::ordinal(tried + 1))
            } else {
                "".into()
            },
        );
        let step: S = match serde_json::from_str(&step)
            .map_err(|e| ErrorReport::DeserializeStep(e, format!("{:?}", step)))
        {
            Ok(x) => x,
            Err(e) => {
                self.set_task_error(id, e.into())
                    .await
                    .map_err(ErrorReport::log)
                    .ok();
                return Ok(());
            }
        };

        let retry_limit = step.retry_limit();
        let retry_delay = step.retry_delay();
        match step.step(&self.db).await {
            Err(e) => {
                self.process_error(id, tried, retry_limit, retry_delay, e)
                    .await?
            }
            Ok(NextStep::None) => self.finish_task(id).await?,
            Ok(NextStep::Now(step)) => self.update_task_step(id, step, Duration::ZERO).await?,
            Ok(NextStep::Delayed(step, delay)) => self.update_task_step(id, step, delay).await?,
        };
        Ok(())
    }

    /// Unlocks all tasks. This is intended to run at the start of the worker as
    /// some tasks could remain locked as running indefinitely if the
    /// previous run ended due to some kind of crash.
    async fn unlock_stale_tasks(&self) -> crate::Result<()> {
        let unlocked =
            sqlx::query!("UPDATE pg_task SET is_running = false WHERE is_running = true")
                .execute(&self.db)
                .await
                .map_err(crate::Error::UnlockStaleTasks)?
                .rows_affected();
        if unlocked == 0 {
            debug!("No stale tasks to unlock")
        } else {
            debug!("Unlocked {} stale tasks", unlocked)
        }
        Ok(())
    }

    /// Waits until the next task is ready, locks it as running and returns it.
    async fn recv_task(&self) -> ReportResult<Task> {
        trace!("Receiving the next task");

        loop {
            let table_changes = self.waiter.subscribe();
            let mut tx = self.db.begin().await.map_err(sqlx_error!("begin"))?;
            if let Some(task) = fetch_closest_task(&mut tx).await? {
                let time_to_run = task.wakeup_at - Utc::now();
                if time_to_run <= chrono::Duration::zero() {
                    mark_task_running(&mut tx, task.id).await?;
                    tx.commit()
                        .await
                        .map_err(sqlx_error!("commit on task return"))?;
                    return Ok(task);
                }
                tx.commit()
                    .await
                    .map_err(sqlx_error!("commit on wait for a period"))?;
                table_changes
                    .wait_for(util::chrono_duration_to_std(time_to_run))
                    .await;
            } else {
                tx.commit()
                    .await
                    .map_err(sqlx_error!("commit on wait forever"))?;
                table_changes.wait_forever().await;
            }
        }
    }

    /// Updates the tasks step
    async fn update_task_step(&self, task_id: Uuid, step: S, delay: Duration) -> ReportResult<()> {
        let step = match serde_json::to_string(&step)
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
        trace!("[{task_id}] update step to {step}");

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
            Utc::now() + util::std_duration_to_chrono(delay),
        )
        .execute(&self.db)
        .await
        .map_err(sqlx_error!())?;

        debug!("[{task_id}] step is done");
        Ok(())
    }

    /// Removes the finished task
    async fn finish_task(&self, task_id: Uuid) -> ReportResult<()> {
        info!("[{task_id}] is successfully completed");
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
    ) -> ReportResult<()> {
        if tried < retry_limit {
            self.retry_task(task_id, tried, retry_limit, retry_delay, err)
                .await
        } else {
            self.set_task_error(task_id, err).await
        }
    }

    /// Schedules the task for retry
    async fn retry_task(
        &self,
        task_id: Uuid,
        tried: i32,
        retry_limit: i32,
        delay: Duration,
        err: StepError,
    ) -> ReportResult<()> {
        trace!("[{task_id}] scheduling a retry");

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
            "[{task_id}] scheduled {attempt} of {retry_limit} retries in {delay:?} on error: {}",
            source_chain::to_string(&*err),
            attempt = util::ordinal(tried + 1)
        );
        Ok(())
    }

    /// Sets the task error
    async fn set_task_error(&self, task_id: Uuid, err: StepError) -> ReportResult<()> {
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
            "[{task_id}] resulted in an error at step {step} after {tried} attempts: {}",
            &err
        );
        Ok(())
    }
}

/// Fetches the closest task to run
async fn fetch_closest_task(con: &mut PgConnection) -> ReportResult<Option<Task>> {
    trace!("Fetching the closest task to run");
    let task = sqlx::query_as!(
        Task,
        r#"
        SELECT
            id,
            step,
            tried,
            wakeup_at
        FROM pg_task
        WHERE is_running = false
          AND error IS NULL
        ORDER BY wakeup_at
        LIMIT 1
        FOR UPDATE
        "#,
    )
    .fetch_optional(con)
    .await
    .map_err(sqlx_error!("select"))?;

    if let Some(ref task) = task {
        let delay = task.delay();
        if delay == Duration::ZERO {
            trace!("[{}] is to run now", task.id);
        } else {
            trace!("[{}] is to run in {:?}", task.id, delay);
        }
    } else {
        debug!("No tasks to run");
    }
    Ok(task)
}

async fn mark_task_running(con: &mut PgConnection, task_id: Uuid) -> ReportResult<()> {
    sqlx::query!(
        "
        UPDATE pg_task
        SET is_running = true,
            updated_at = now()
        WHERE id = $1
        ",
        task_id
    )
    .execute(con)
    .await
    .map_err(sqlx_error!())?;
    Ok(())
}
