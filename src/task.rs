use crate::{
    util::{chrono_duration_to_std, db_error, ordinal, std_duration_to_chrono},
    Error, NextStep, Result, Step, StepError,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{
    postgres::{PgConnection, PgPool},
    types::Uuid,
    PgExecutor,
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
    /// Returns a delay before running the task
    pub fn wait_before_running(&self) -> Option<Duration> {
        let delay = self.wakeup_at - Utc::now();
        if delay <= chrono::Duration::zero() {
            None
        } else {
            Some(chrono_duration_to_std(delay))
        }
    }

    /// Fetches the closest task to run
    pub async fn fetch_closest(con: &mut PgConnection) -> Result<Option<Self>> {
        trace!("Fetching the closest task to run");
        sqlx::query_as!(
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
        .map_err(db_error!())
    }

    /// Marks the task running
    pub async fn mark_running(&self, con: &mut PgConnection) -> Result<()> {
        trace!("[{}] mark running", self.id);
        sqlx::query!(
            "UPDATE pg_task SET is_running = true WHERE id = $1",
            self.id
        )
        .execute(con)
        .await
        .map_err(db_error!())?;
        Ok(())
    }

    /// Deserializes the current task step and marks it running.
    /// If deserialization fails, stores the error instead and leaves the task
    /// non-running.
    pub(crate) async fn claim<S: Step<S>>(&self, con: &mut PgConnection) -> Result<Option<S>> {
        let step = match self.parse_step() {
            Ok(step) => step,
            Err(e) => {
                self.save_error(&mut *con, e.into(), false).await?;
                return Ok(None);
            }
        };

        self.mark_running(con).await?;
        Ok(Some(step))
    }

    /// Runs the current step of the task to completion
    pub async fn run_step<S: Step<S>>(&self, db: &PgPool, step: S) -> Result<()> {
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
                    self.retry(db, self.tried, retry_limit, retry_delay, e)
                        .await?;
                } else {
                    self.save_error(db, e, true).await?;
                }
            }
            Ok(NextStep::None) => self.complete(db).await?,
            Ok(NextStep::Now(step)) => self.save_next_step(db, step, Duration::ZERO).await?,
            Ok(NextStep::Delayed(step, delay)) => self.save_next_step(db, step, delay).await?,
        };
        Ok(())
    }

    fn parse_step<S: Step<S>>(&self) -> Result<S> {
        serde_json::from_str(&self.step)
            .map_err(|e| Error::DeserializeStep(e, self.step.to_string()))
    }

    /// Saves the task error
    async fn save_error<'e, E>(&self, db: E, err: StepError, increment_tried: bool) -> Result<()>
    where
        E: PgExecutor<'e>,
    {
        let err_str = source_chain::to_string(&*err);
        let tried_increment = if increment_tried { 1 } else { 0 };
        let step = sqlx::query!(
            r#"
            UPDATE pg_task
            SET is_running = false,
                tried = tried + $3,
                error = $2,
                wakeup_at = now()
            WHERE id = $1
            RETURNING step::TEXT as "step!"
            "#,
            self.id,
            &err_str,
            tried_increment,
        )
        .fetch_one(db)
        .await
        .map(|r| r.step)
        .map_err(db_error!())?;

        if increment_tried {
            let attempt = self.tried + 1;
            error!(
                "[{id}] resulted in an error at step {step} on {attempt} attempt: {err_str}",
                id = self.id,
                attempt = ordinal(attempt)
            );
        } else {
            error!(
                "[{id}] couldn't deserialize step {step}: {err_str}",
                id = self.id
            );
        }

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
            .map_err(|e| Error::SerializeStep(e, format!("{step:?}")))
        {
            Ok(x) => x,
            Err(e) => return self.save_error(db, e.into(), true).await,
        };
        debug!("[{}] moved to the next step {step}", self.id);

        sqlx::query!(
            "
            UPDATE pg_task
            SET is_running = false,
                tried = 0,
                step = $2,
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

    /// Schedules the task for retry
    async fn retry(
        &self,
        db: &PgPool,
        tried: i32,
        retry_limit: i32,
        delay: Duration,
        err: StepError,
    ) -> Result<()> {
        let delay = std_duration_to_chrono(delay);
        debug!(
            "[{id}] scheduled {attempt} of {retry_limit} retries in {delay:?} on error: {err}",
            id = self.id,
            attempt = ordinal(tried + 1),
            err = source_chain::to_string(&*err),
        );

        sqlx::query!(
            "
            UPDATE pg_task
            SET is_running = false,
                tried = tried + 1,
                wakeup_at = $2
            WHERE id = $1
            ",
            self.id,
            Utc::now() + delay,
        )
        .execute(db)
        .await
        .map_err(db_error!())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Task;
    use crate::{NextStep, Step};
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use sqlx::{types::Uuid, PgPool, Row};
    use std::{io, time::Duration};

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct Valid;

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct AdvanceNow {
        value: i32,
    }

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct AdvanceLater {
        value: i32,
        delay_ms: u64,
    }

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct Followup {
        value: i32,
    }

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct RetryFail;

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct BrokenNext;

    #[derive(Debug)]
    pub(super) struct Unserializable;

    crate::task!(TestTask {
        Valid,
        AdvanceNow,
        AdvanceLater,
        Followup,
        RetryFail,
        BrokenNext,
        Unserializable,
    });

    #[async_trait::async_trait]
    impl Step<TestTask> for Valid {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            Ok(NextStep::None)
        }
    }

    #[async_trait::async_trait]
    impl Step<TestTask> for AdvanceNow {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            NextStep::now(Followup {
                value: self.value + 1,
            })
        }
    }

    #[async_trait::async_trait]
    impl Step<TestTask> for AdvanceLater {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            NextStep::delay(
                Followup {
                    value: self.value + 1,
                },
                Duration::from_millis(self.delay_ms),
            )
        }
    }

    #[async_trait::async_trait]
    impl Step<TestTask> for Followup {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            NextStep::none()
        }
    }

    #[async_trait::async_trait]
    impl Step<TestTask> for RetryFail {
        const RETRY_LIMIT: i32 = 2;
        const RETRY_DELAY: Duration = Duration::from_millis(250);

        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            Err(io::Error::other("retryable failure").into())
        }
    }

    #[async_trait::async_trait]
    impl Step<TestTask> for BrokenNext {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            NextStep::now(Unserializable)
        }
    }

    impl serde::Serialize for Unserializable {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("can't serialize test step"))
        }
    }

    impl<'de> serde::Deserialize<'de> for Unserializable {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            <() as serde::Deserialize>::deserialize(deserializer)?;
            Ok(Self)
        }
    }

    #[async_trait::async_trait]
    impl Step<TestTask> for Unserializable {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            unreachable!("the test never executes the unserializable step")
        }
    }

    #[derive(Debug)]
    struct TaskRow {
        step: String,
        wakeup_at: DateTime<Utc>,
        tried: i32,
        is_running: bool,
        error: Option<String>,
    }

    fn serialized_step(step: &TestTask) -> String {
        serde_json::to_string(step).unwrap()
    }

    async fn insert_task_row(
        pool: &PgPool,
        step: &str,
        wakeup_at: DateTime<Utc>,
        tried: i32,
        is_running: bool,
        error: Option<&str>,
    ) -> Uuid {
        sqlx::query(
            "
            INSERT INTO pg_task (step, wakeup_at, tried, is_running, error)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            ",
        )
        .bind(step)
        .bind(wakeup_at)
        .bind(tried)
        .bind(is_running)
        .bind(error)
        .fetch_one(pool)
        .await
        .unwrap()
        .get("id")
    }

    async fn insert_task(pool: &PgPool, step: &TestTask, tried: i32, is_running: bool) -> Uuid {
        let step = serialized_step(step);
        insert_task_row(pool, &step, Utc::now(), tried, is_running, None).await
    }

    async fn claim_task(pool: &PgPool, step: TestTask, tried: i32) -> (Task, TestTask) {
        let id = insert_task(pool, &step, tried, false).await;
        let mut tx = pool.begin().await.unwrap();
        let task = Task::fetch_closest(&mut tx).await.unwrap().unwrap();
        assert_eq!(task.id, id);
        let claimed = task.claim::<TestTask>(&mut tx).await.unwrap().unwrap();
        tx.commit().await.unwrap();
        (task, claimed)
    }

    async fn fetch_task_row(pool: &PgPool, id: Uuid) -> Option<TaskRow> {
        sqlx::query(
            "
            SELECT step, wakeup_at, tried, is_running, error
            FROM pg_task
            WHERE id = $1
            ",
        )
        .bind(id)
        .fetch_optional(pool)
        .await
        .unwrap()
        .map(|row| TaskRow {
            step: row.get("step"),
            wakeup_at: row.get("wakeup_at"),
            tried: row.get("tried"),
            is_running: row.get("is_running"),
            error: row.get("error"),
        })
    }

    fn assert_timestamp_between(
        actual: DateTime<Utc>,
        earliest: DateTime<Utc>,
        latest: DateTime<Utc>,
    ) {
        assert!(
            actual >= earliest,
            "timestamp {actual:?} should be after {earliest:?}",
        );
        assert!(
            actual <= latest,
            "timestamp {actual:?} should be before {latest:?}",
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn claim_marks_invalid_steps_errored(pool: PgPool) {
        sqlx::query("INSERT INTO pg_task (step, wakeup_at) VALUES ($1, $2)")
            .bind("not-json")
            .bind(Utc::now())
            .execute(&pool)
            .await
            .unwrap();

        let mut tx = pool.begin().await.unwrap();
        let task = Task::fetch_closest(&mut tx).await.unwrap().unwrap();

        assert!(task.claim::<TestTask>(&mut tx).await.unwrap().is_none());

        tx.commit().await.unwrap();

        let row = sqlx::query("SELECT tried, is_running, error FROM pg_task LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(row.get::<i32, _>("tried"), 0);
        assert!(!row.get::<bool, _>("is_running"));
        assert!(row.get::<Option<String>, _>("error").is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn claim_marks_valid_steps_running(pool: PgPool) {
        let id = insert_task(&pool, &TestTask::Valid(Valid), 0, false).await;

        let mut tx = pool.begin().await.unwrap();
        let task = Task::fetch_closest(&mut tx).await.unwrap().unwrap();
        let claimed = task.claim::<TestTask>(&mut tx).await.unwrap();
        tx.commit().await.unwrap();

        assert!(matches!(claimed, Some(TestTask::Valid(Valid))));

        let row = fetch_task_row(&pool, id).await.unwrap();
        assert_eq!(row.step, serialized_step(&TestTask::Valid(Valid)));
        assert_eq!(row.tried, 0);
        assert!(row.is_running);
        assert!(row.error.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn fetch_closest_ignores_running_and_errored_tasks_and_picks_the_earliest_eligible_one(
        pool: PgPool,
    ) {
        let now = Utc::now();
        let valid = serialized_step(&TestTask::Valid(Valid));

        insert_task_row(
            &pool,
            &valid,
            now - ChronoDuration::seconds(3),
            0,
            true,
            None,
        )
        .await;
        insert_task_row(
            &pool,
            &valid,
            now - ChronoDuration::seconds(2),
            0,
            false,
            Some("boom"),
        )
        .await;
        let expected = insert_task_row(
            &pool,
            &valid,
            now - ChronoDuration::seconds(1),
            0,
            false,
            None,
        )
        .await;
        insert_task_row(
            &pool,
            &valid,
            now + ChronoDuration::seconds(1),
            0,
            false,
            None,
        )
        .await;

        let mut tx = pool.begin().await.unwrap();
        let task = Task::fetch_closest(&mut tx).await.unwrap().unwrap();
        tx.commit().await.unwrap();

        assert_eq!(task.id, expected);
        assert_eq!(task.step, valid);
        assert_eq!(task.tried, 0);
        assert!(task.wait_before_running().is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_step_completes_tasks(pool: PgPool) {
        let (task, step) = claim_task(&pool, TestTask::Valid(Valid), 0).await;

        task.run_step(&pool, step).await.unwrap();

        assert!(fetch_task_row(&pool, task.id).await.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_step_saves_immediate_next_step_and_resets_retries(pool: PgPool) {
        let (task, step) =
            claim_task(&pool, TestTask::AdvanceNow(AdvanceNow { value: 41 }), 2).await;
        let started_at = Utc::now();

        task.run_step(&pool, step).await.unwrap();

        let finished_at = Utc::now();
        let row = fetch_task_row(&pool, task.id).await.unwrap();
        assert_eq!(
            row.step,
            serialized_step(&TestTask::Followup(Followup { value: 42 })),
        );
        assert_eq!(row.tried, 0);
        assert!(!row.is_running);
        assert!(row.error.is_none());
        assert_timestamp_between(
            row.wakeup_at,
            started_at,
            finished_at + ChronoDuration::seconds(1),
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_step_saves_delayed_next_step_and_resets_retries(pool: PgPool) {
        let delay = Duration::from_millis(250);
        let (task, step) = claim_task(
            &pool,
            TestTask::AdvanceLater(AdvanceLater {
                value: 9,
                delay_ms: delay.as_millis() as u64,
            }),
            3,
        )
        .await;
        let started_at = Utc::now();

        task.run_step(&pool, step).await.unwrap();

        let finished_at = Utc::now();
        let row = fetch_task_row(&pool, task.id).await.unwrap();
        let delay = ChronoDuration::from_std(delay).unwrap();
        assert_eq!(
            row.step,
            serialized_step(&TestTask::Followup(Followup { value: 10 })),
        );
        assert_eq!(row.tried, 0);
        assert!(!row.is_running);
        assert!(row.error.is_none());
        assert_timestamp_between(
            row.wakeup_at,
            started_at + delay,
            finished_at + delay + ChronoDuration::seconds(1),
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_step_schedules_retries_before_the_retry_limit(pool: PgPool) {
        let retry_delay = <RetryFail as Step<TestTask>>::RETRY_DELAY;
        let (task, step) = claim_task(&pool, TestTask::RetryFail(RetryFail), 1).await;
        let started_at = Utc::now();

        task.run_step(&pool, step).await.unwrap();

        let finished_at = Utc::now();
        let row = fetch_task_row(&pool, task.id).await.unwrap();
        let retry_delay = ChronoDuration::from_std(retry_delay).unwrap();
        assert_eq!(row.step, serialized_step(&TestTask::RetryFail(RetryFail)));
        assert_eq!(row.tried, 2);
        assert!(!row.is_running);
        assert!(row.error.is_none());
        assert_timestamp_between(
            row.wakeup_at,
            started_at + retry_delay,
            finished_at + retry_delay + ChronoDuration::seconds(1),
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_step_saves_terminal_errors_after_retry_limit(pool: PgPool) {
        let retry_limit = <RetryFail as Step<TestTask>>::RETRY_LIMIT;
        let (task, step) = claim_task(&pool, TestTask::RetryFail(RetryFail), retry_limit).await;
        let started_at = Utc::now();

        task.run_step(&pool, step).await.unwrap();

        let finished_at = Utc::now();
        let row = fetch_task_row(&pool, task.id).await.unwrap();
        assert_eq!(row.step, serialized_step(&TestTask::RetryFail(RetryFail)));
        assert_eq!(row.tried, retry_limit + 1);
        assert!(!row.is_running);
        assert!(row
            .error
            .as_deref()
            .is_some_and(|error| error.contains("retryable failure")));
        assert_timestamp_between(
            row.wakeup_at,
            started_at,
            finished_at + ChronoDuration::seconds(1),
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_step_saves_next_step_serialization_failures_as_errors(pool: PgPool) {
        let (task, step) = claim_task(&pool, TestTask::BrokenNext(BrokenNext), 0).await;

        task.run_step(&pool, step).await.unwrap();

        let row = fetch_task_row(&pool, task.id).await.unwrap();
        assert_eq!(row.step, serialized_step(&TestTask::BrokenNext(BrokenNext)),);
        assert_eq!(row.tried, 1);
        assert!(!row.is_running);
        assert!(row
            .error
            .as_deref()
            .is_some_and(|error| error.contains("can't serialize test step")));
    }
}
