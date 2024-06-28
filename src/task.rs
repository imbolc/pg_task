use crate::{
    util::{chrono_duration_to_std, db_error, ordinal, std_duration_to_chrono},
    Error, NextStep, Result, Step, StepError,
};

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{
    postgres::{PgConnection, PgPool},
    types::Uuid,
};
use std::{fmt, time::Duration};
use tracing::{debug, error, info, trace};

#[derive(Debug)]
pub struct Task {
    pub id: Uuid,
    step: String,
    tried: i32,
    pub wakeup_at: DateTime<Utc>,
}

impl Task {
    /// Fetches the closest task to run
    pub async fn fetch_closest(con: &mut PgConnection) -> Result<Option<Self>> {
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
        .map_err(db_error!("select"))?;

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

    pub async fn mark_running(&self, con: &mut PgConnection) -> Result<()> {
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
        .map_err(db_error!())?;
        Ok(())
    }

    /// Returns the delay time before running the task
    fn delay(&self) -> Duration {
        let delay = self.wakeup_at - Utc::now();
        if delay <= chrono::Duration::zero() {
            Duration::ZERO
        } else {
            chrono_duration_to_std(delay)
        }
    }

    /// Runs the current step of the task to completion
    pub async fn run_step<S: Step<S>>(&self, db: &PgPool) -> Result<()> {
        info!(
            "[{}]{} run step {}",
            self.id,
            if self.tried > 0 {
                format!(" {} attempt to", ordinal(self.tried + 1))
            } else {
                "".into()
            },
            self.step
        );
        let step: S = match serde_json::from_str(&self.step)
            .map_err(|e| Error::DeserializeStep(e, format!("{:?}", self.step)))
        {
            Ok(x) => x,
            Err(e) => {
                self.save_error(db, e.into()).await.ok();
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
    async fn save_error(&self, db: &PgPool, err: StepError) -> Result<()> {
        let err_str = source_chain::to_string(&*err);

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
            &err_str,
            Utc::now(),
        )
        .fetch_one(db)
        .await
        .map(|r| (r.tried, r.step))
        .map_err(db_error!())?;

        error!(
            "[{id}] resulted in an error at step {step} on {attempt} attempt: {err_str}",
            id = self.id,
            attempt = ordinal(tried + 1)
        );
        Ok(())
    }

    /// Updates the tasks step
    async fn save_next_step(
        &self,
        db: &PgPool,
        step: impl Serialize + fmt::Debug,
        delay: Duration,
    ) -> Result<()> {
        let step = match serde_json::to_string(&step)
            .map_err(|e| Error::SerializeStep(e, format!("{:?}", step)))
        {
            Ok(x) => x,
            Err(e) => {
                self.save_error(db, e.into()).await.ok();
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
            Utc::now() + std_duration_to_chrono(delay),
        )
        .execute(db)
        .await
        .map_err(db_error!())?;

        debug!("[{}] step is done", self.id);
        Ok(())
    }

    /// Removes the finished task
    async fn complete(&self, db: &PgPool) -> Result<()> {
        info!("[{}] is successfully completed", self.id);
        sqlx::query!("DELETE FROM pg_task WHERE id = $1", self.id)
            .execute(db)
            .await
            .map_err(db_error!())?;
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
    ) -> Result<()> {
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
    ) -> Result<()> {
        trace!("[{}] scheduling a retry", self.id);

        let delay = std_duration_to_chrono(delay);
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
        .map_err(db_error!())?;

        debug!(
            "[{id}] scheduled {attempt} of {retry_limit} retries in {delay:?} on error: {err}",
            id = self.id,
            attempt = ordinal(tried + 1),
            err = source_chain::to_string(&*err),
        );
        Ok(())
    }
}
