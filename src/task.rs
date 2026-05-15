use crate::{
    util::{db_error, ordinal, std_duration_to_chrono},
    Error, NextStep, Result, Step, StepError,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{
    postgres::{types::PgInterval, PgConnection, PgPool},
    PgExecutor,
};
use std::{fmt, time::Duration};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

#[derive(Debug)]
pub struct Task {
    pub id: Uuid,
    step: String,
    tried: i32,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct WorkerLease {
    worker_id: Uuid,
    timeout: PgInterval,
}

impl WorkerLease {
    pub(crate) fn new(worker_id: Uuid, timeout: Duration) -> Self {
        Self {
            worker_id,
            timeout: duration_to_pg_interval(timeout),
        }
    }
}

fn duration_to_pg_interval(duration: Duration) -> PgInterval {
    let microseconds = duration.as_nanos().div_ceil(1_000);
    PgInterval {
        months: 0,
        days: 0,
        microseconds: microseconds.min(i64::MAX as u128) as i64,
    }
}

impl Task {
    /// Returns a delay before a task should run at the given time.
    pub fn delay_until(wakeup_at: DateTime<Utc>) -> Option<Duration> {
        (wakeup_at - Utc::now())
            .to_std()
            .ok()
            .filter(|delay| !delay.is_zero())
    }

    /// Fetches the closest ready task to run.
    pub async fn fetch_ready(con: &mut PgConnection) -> Result<Option<Self>> {
        trace!("Fetching the closest ready task to run");
        sqlx::query_as!(
            Task,
            r#"
            SELECT
                id,
                step,
                tried
            FROM pg_task
            WHERE error IS NULL
              AND (
                CASE
                    WHEN locked_by IS NOT NULL THEN
                        GREATEST(wakeup_at, lock_expires_at)
                    ELSE
                        wakeup_at
                END
              ) <= now()
            ORDER BY
                CASE
                    WHEN locked_by IS NOT NULL THEN
                        GREATEST(wakeup_at, lock_expires_at)
                    ELSE
                        wakeup_at
                END
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .fetch_optional(con)
        .await
        .map_err(db_error!())
    }

    /// Fetches the closest time when a task may become claimable.
    pub async fn fetch_next_available_at(con: &mut PgConnection) -> Result<Option<DateTime<Utc>>> {
        trace!("Fetching the closest task availability time");
        sqlx::query_scalar!(
            r#"
            SELECT
                CASE
                    WHEN locked_by IS NOT NULL THEN
                        GREATEST(wakeup_at, lock_expires_at)
                    ELSE
                        wakeup_at
                END AS "next_at!"
            FROM pg_task
            WHERE error IS NULL
            ORDER BY 1
            LIMIT 1
            "#,
        )
        .fetch_optional(con)
        .await
        .map_err(db_error!())
    }

    /// Claims the task lease for this worker.
    pub(crate) async fn claim_lease(
        &self,
        con: &mut PgConnection,
        lease: WorkerLease,
    ) -> Result<()> {
        trace!("[{}] claim lease", self.id);
        sqlx::query!(
            r#"
            UPDATE pg_task
            SET locked_by = $2,
                lock_expires_at = now() + $3::interval
            WHERE id = $1
            "#,
            self.id,
            lease.worker_id,
            lease.timeout,
        )
        .execute(con)
        .await
        .map_err(db_error!("claim lease"))?;
        Ok(())
    }

    /// Renews live task leases owned by a worker.
    pub(crate) async fn renew_leases(
        db: &PgPool,
        lease: WorkerLease,
        task_ids: &[Uuid],
    ) -> Result<Vec<Uuid>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        trace!("Renewing task leases for worker {}", lease.worker_id);
        sqlx::query!(
            r#"
            UPDATE pg_task
            SET lock_expires_at = now() + $2::interval
            WHERE locked_by = $1
              AND id = ANY($3)
              AND lock_expires_at > now()
              AND error IS NULL
            RETURNING id
            "#,
            lease.worker_id,
            lease.timeout,
            task_ids,
        )
        .fetch_all(db)
        .await
        .map(|rows| rows.into_iter().map(|row| row.id).collect())
        .map_err(db_error!("renew leases"))
    }

    /// Deserializes the current task step and claims its lease.
    /// If deserialization fails, stores the error instead and leaves the task
    /// unleased.
    pub(crate) async fn claim<S: Step<S>>(
        &self,
        con: &mut PgConnection,
        lease: WorkerLease,
    ) -> Result<Option<S>> {
        let step = match self.parse_step() {
            Ok(step) => step,
            Err(e) => {
                self.save_claim_error(&mut *con, e.into()).await?;
                return Ok(None);
            }
        };

        self.claim_lease(con, lease).await?;
        Ok(Some(step))
    }

    /// Runs the current step of the task to completion
    pub(crate) async fn run_step<S: Step<S>>(
        &self,
        db: &PgPool,
        step: S,
        lease: WorkerLease,
    ) -> Result<()> {
        info!(
            "[{id}]{attempt} run step {step}",
            id = self.id,
            attempt = if self.tried > 0 {
                format!(" {} attempt to", ordinal(self.tried + 1))
            } else {
                "".into()
            },
            step = self.step
        );
        let retry_limit = step.retry_limit();
        let retry_delay = step.retry_delay();
        match step.step(db).await {
            Err(e) => {
                if self.tried < retry_limit {
                    self.retry(db, self.tried, retry_limit, retry_delay, e, lease)
                        .await?;
                } else {
                    self.save_step_error(db, e, lease).await?;
                }
            }
            Ok(NextStep::None) => self.complete(db, lease).await?,
            Ok(NextStep::Now(step)) => {
                self.save_next_step(db, step, Duration::ZERO, lease).await?;
            }
            Ok(NextStep::Delayed(step, delay)) => {
                self.save_next_step(db, step, delay, lease).await?;
            }
        };
        Ok(())
    }

    fn parse_step<S: Step<S>>(&self) -> Result<S> {
        serde_json::from_str(&self.step)
            .map_err(|e| Error::DeserializeStep(e, self.step.to_string()))
    }

    /// Saves a deserialization error for a task before the worker owns it.
    async fn save_claim_error<'e, E>(&self, db: E, err: StepError) -> Result<()>
    where
        E: PgExecutor<'e>,
    {
        let err_str = source_chain::to_string(&*err);
        let step = sqlx::query!(
            r#"
            UPDATE pg_task
            SET error = $2,
                wakeup_at = now(),
                locked_by = NULL,
                lock_expires_at = NULL
            WHERE id = $1
            RETURNING step::TEXT as "step!"
            "#,
            self.id,
            &err_str,
        )
        .fetch_one(db)
        .await
        .map(|r| r.step)
        .map_err(db_error!())?;

        error!(
            "[{id}] couldn't deserialize step {step}: {err_str}",
            id = self.id
        );

        Ok(())
    }

    /// Saves the task error if the worker still owns the task.
    async fn save_step_error(&self, db: &PgPool, err: StepError, lease: WorkerLease) -> Result<()> {
        let err_str = source_chain::to_string(&*err);
        let updated_task = sqlx::query!(
            r#"
            UPDATE pg_task
            SET tried = tried + 1,
                error = $2,
                wakeup_at = now(),
                locked_by = NULL,
                lock_expires_at = NULL
            WHERE id = $1
              AND locked_by = $3
              AND lock_expires_at > now()
            RETURNING step::TEXT as "step!"
            "#,
            self.id,
            &err_str,
            lease.worker_id,
        )
        .fetch_optional(db)
        .await
        .map_err(db_error!())?;

        let Some(updated_task) = updated_task else {
            self.log_lost_lease(lease.worker_id, "save the step error");
            return Ok(());
        };

        let attempt = self.tried + 1;
        error!(
            "[{id}] resulted in an error at step {step} on {attempt} attempt: {err_str}",
            id = self.id,
            step = updated_task.step,
            attempt = ordinal(attempt)
        );

        Ok(())
    }

    /// Updates the tasks step
    async fn save_next_step(
        &self,
        db: &PgPool,
        step: impl Serialize + fmt::Debug,
        delay: Duration,
        lease: WorkerLease,
    ) -> Result<()> {
        let step = match serde_json::to_string(&step)
            .map_err(|e| Error::SerializeStep(e, format!("{step:?}")))
        {
            Ok(x) => x,
            Err(e) => return self.save_step_error(db, e.into(), lease).await,
        };
        let result = sqlx::query!(
            "
            UPDATE pg_task
            SET tried = 0,
                step = $2,
                wakeup_at = $3,
                locked_by = NULL,
                lock_expires_at = NULL
            WHERE id = $1
              AND locked_by = $4
              AND lock_expires_at > now()
            ",
            self.id,
            step,
            Utc::now() + std_duration_to_chrono(delay),
            lease.worker_id,
        )
        .execute(db)
        .await
        .map_err(db_error!())?;
        if result.rows_affected() == 0 {
            self.log_lost_lease(lease.worker_id, "save the next step");
        } else {
            debug!("[{}] moved to the next step {step}", self.id);
        }
        Ok(())
    }

    /// Removes the finished task
    async fn complete(&self, db: &PgPool, lease: WorkerLease) -> Result<()> {
        let result = sqlx::query!(
            r#"
            DELETE FROM pg_task
            WHERE id = $1
              AND locked_by = $2
              AND lock_expires_at > now()
            "#,
            self.id,
            lease.worker_id,
        )
        .execute(db)
        .await
        .map_err(db_error!())?;
        if result.rows_affected() == 0 {
            self.log_lost_lease(lease.worker_id, "complete the task");
        } else {
            info!("[{}] is successfully completed", self.id);
        }
        Ok(())
    }

    /// Schedules the task for retry
    async fn retry(
        &self,
        db: &PgPool,
        tried: i32,
        retry_limit: i32,
        delay: Duration,
        err: StepError,
        lease: WorkerLease,
    ) -> Result<()> {
        let delay = std_duration_to_chrono(delay);

        let result = sqlx::query!(
            "
            UPDATE pg_task
            SET tried = tried + 1,
                wakeup_at = $2,
                locked_by = NULL,
                lock_expires_at = NULL
            WHERE id = $1
              AND locked_by = $3
              AND lock_expires_at > now()
            ",
            self.id,
            Utc::now() + delay,
            lease.worker_id,
        )
        .execute(db)
        .await
        .map_err(db_error!())?;
        if result.rows_affected() == 0 {
            self.log_lost_lease(lease.worker_id, "schedule a retry");
        } else {
            debug!(
                "[{id}] scheduled {attempt} of {retry_limit} retries in {delay:?} on error: {err}",
                id = self.id,
                attempt = ordinal(tried + 1),
                err = source_chain::to_string(&*err),
            );
        }

        Ok(())
    }

    fn log_lost_lease(&self, worker_id: Uuid, action: &str) {
        warn!(
            "[{}] couldn't {action} because worker {worker_id}'s lease expired or is no longer owned by this worker",
            self.id
        );
    }
}

#[cfg(test)]
mod tests;
