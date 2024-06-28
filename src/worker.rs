use crate::{util, waiter::Waiter, NextStep, Step, StepError, LOST_CONNECTION_SLEEP};
use chrono::{DateTime, Utc};
use code_path::code_path;
use serde::Serialize;
use sqlx::{
    postgres::{PgConnection, PgPool},
    types::Uuid,
};
use std::{fmt, marker::PhantomData, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, time::sleep};
use tracing::{debug, error, info, trace, warn};

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
    concurrency: usize,
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

impl<S: Step<S>> Worker<S> {
    /// Creates a new worker
    pub fn new(db: PgPool) -> Self {
        let waiter = Waiter::new();
        let concurrency = num_cpus::get();
        Self {
            db,
            waiter,
            concurrency,
            tasks: PhantomData,
        }
    }

    /// Sets the number of concurrent tasks, default is the number of CPUs
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Runs all ready tasks to completion and waits for new ones
    pub async fn run(&self) -> crate::Result<()> {
        self.unlock_stale_tasks().await?;
        self.waiter.listen(self.db.clone()).await?;

        let semaphore = Arc::new(Semaphore::new(self.concurrency));
        loop {
            match self.recv_task().await {
                Ok(task) => {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(crate::Error::UnreachableWorkerSemaphoreClosed)?;
                    let db = self.db.clone();
                    tokio::spawn(async move {
                        task.run_step::<S>(&db).await.map_err(ErrorReport::log).ok();
                        drop(permit);
                    });
                }
                Err(e) => {
                    warn!(
                        "Can't fetch a task (probably due to db connection loss):\n{}",
                        source_chain::to_string(&e)
                    );
                    sleep(LOST_CONNECTION_SLEEP).await;
                    util::wait_for_reconnection(&self.db, LOST_CONNECTION_SLEEP).await;
                    warn!("Task fetching is probably restored");
                }
            }
        }
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

    /// Waits until the next task is ready, marks it as running and returns it.
    async fn recv_task(&self) -> ReportResult<Task> {
        trace!("Receiving the next task");

        loop {
            let table_changes = self.waiter.subscribe();
            let mut tx = self.db.begin().await.map_err(sqlx_error!("begin"))?;
            if let Some(task) = Task::fetch_closest(&mut tx).await? {
                let time_to_run = task.wakeup_at - Utc::now();
                if time_to_run <= chrono::Duration::zero() {
                    task.mark_running(&mut tx).await?;
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
}

impl Task {
    /// Fetches the closest task to run
    async fn fetch_closest(con: &mut PgConnection) -> ReportResult<Option<Self>> {
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

    async fn mark_running(&self, con: &mut PgConnection) -> ReportResult<()> {
        sqlx::query!(
            "
        UPDATE pg_task
        SET is_running = true,
            updated_at = now()
        WHERE id = $1
        ",
            self.id
        )
        .execute(con)
        .await
        .map_err(sqlx_error!())?;
        Ok(())
    }

    /// Returns the delay time before running the task
    fn delay(&self) -> Duration {
        let delay = self.wakeup_at - Utc::now();
        if delay <= chrono::Duration::zero() {
            Duration::ZERO
        } else {
            util::chrono_duration_to_std(delay)
        }
    }

    /// Runs the current step of the task to completion
    async fn run_step<S: Step<S>>(&self, db: &PgPool) -> ReportResult<()> {
        info!(
            "[{}]{} run step {}",
            self.id,
            if self.tried > 0 {
                format!(" {} attempt to", util::ordinal(self.tried + 1))
            } else {
                "".into()
            },
            self.step
        );
        let step: S = match serde_json::from_str(&self.step)
            .map_err(|e| ErrorReport::DeserializeStep(e, format!("{:?}", self.step)))
        {
            Ok(x) => x,
            Err(e) => {
                self.save_error(db, e.into())
                    .await
                    .map_err(ErrorReport::log)
                    .ok();
                return Ok(());
            }
        };

        let retry_limit = step.retry_limit();
        let retry_delay = step.retry_delay();
        match step.step(db).await {
            Err(e) => {
                self.process_error(db, self.tried, retry_limit, retry_delay, e)
                    .await?
            }
            Ok(NextStep::None) => self.complete(db).await?,
            Ok(NextStep::Now(step)) => self.save_next_step(db, step, Duration::ZERO).await?,
            Ok(NextStep::Delayed(step, delay)) => self.save_next_step(db, step, delay).await?,
        };
        Ok(())
    }

    /// Saves the task error
    async fn save_error(&self, db: &PgPool, err: StepError) -> ReportResult<()> {
        trace!("[{}] saving error", self.id);

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
            self.id,
            &err,
            Utc::now(),
        )
        .fetch_one(db)
        .await
        .map(|r| (r.tried, r.step))
        .map_err(sqlx_error!())?;
        error!(
            "[{}] resulted in an error at step {step} after {tried} attempts: {}",
            self.id, err
        );
        Ok(())
    }

    /// Updates the tasks step
    async fn save_next_step(
        &self,
        db: &PgPool,
        step: impl Serialize + fmt::Debug,
        delay: Duration,
    ) -> ReportResult<()> {
        let step = match serde_json::to_string(&step)
            .map_err(|e| ErrorReport::SerializeStep(e, format!("{:?}", step)))
        {
            Ok(x) => x,
            Err(e) => {
                self.save_error(db, e.into())
                    .await
                    .map_err(ErrorReport::log)
                    .ok();
                return Ok(());
            }
        };

        trace!("[{}] moved to the next step {step}", self.id);
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
            self.id,
            step,
            Utc::now() + util::std_duration_to_chrono(delay),
        )
        .execute(db)
        .await
        .map_err(sqlx_error!())?;

        debug!("[{}] step is done", self.id);
        Ok(())
    }

    /// Removes the finished task
    async fn complete(&self, db: &PgPool) -> ReportResult<()> {
        info!("[{}] is successfully completed", self.id);
        sqlx::query!("DELETE FROM pg_task WHERE id = $1", self.id)
            .execute(db)
            .await
            .map_err(sqlx_error!())?;
        Ok(())
    }

    /// Dealing with the step error
    async fn process_error(
        &self,
        db: &PgPool,
        tried: i32,
        retry_limit: i32,
        retry_delay: Duration,
        err: StepError,
    ) -> ReportResult<()> {
        if tried < retry_limit {
            self.retry(db, tried, retry_limit, retry_delay, err).await
        } else {
            self.save_error(db, err).await
        }
    }

    /// Schedules the task for retry
    async fn retry(
        &self,
        db: &PgPool,
        tried: i32,
        retry_limit: i32,
        delay: Duration,
        err: StepError,
    ) -> ReportResult<()> {
        trace!("[{}] scheduling a retry", self.id);

        let delay = util::std_duration_to_chrono(delay);
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
            self.id,
            wakeup_at,
        )
        .execute(db)
        .await
        .map_err(sqlx_error!())?;

        debug!(
            "[{}] scheduled {attempt} of {retry_limit} retries in {delay:?} on error: {}",
            self.id,
            source_chain::to_string(&*err),
            attempt = util::ordinal(tried + 1)
        );
        Ok(())
    }
}
