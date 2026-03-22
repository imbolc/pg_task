use crate::{util::std_duration_to_chrono, Error, StepResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{types::Uuid, PgExecutor, PgPool};
use std::{fmt, time::Duration};

/// A trait to implement on each task step
#[async_trait]
pub trait Step<Task>
where
    Task: Sized,
    Self: Into<Task> + Send + Sized + fmt::Debug + DeserializeOwned + Serialize,
{
    /// How many times to retry the step on an error
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

/// A trait to implement on the outer enum wrapper containing all the tasks
#[async_trait]
pub trait Scheduler: fmt::Debug + DeserializeOwned + Serialize + Sized + Sync {
    /// Enqueues the task to be run immediately
    async fn enqueue<'e>(&self, db: impl PgExecutor<'e>) -> crate::Result<Uuid> {
        self.schedule(db, Utc::now()).await
    }

    /// Schedules a task to be run after a specified delay
    async fn delay<'e>(&self, db: impl PgExecutor<'e>, delay: Duration) -> crate::Result<Uuid> {
        let delay = std_duration_to_chrono(delay);
        self.schedule(db, Utc::now() + delay).await
    }

    /// Schedules a task to run at a specified time in the future
    async fn schedule<'e>(
        &self,
        db: impl PgExecutor<'e>,
        at: DateTime<Utc>,
    ) -> crate::Result<Uuid> {
        let step = serde_json::to_string(self)
            .map_err(|e| Error::SerializeStep(e, format!("{self:?}")))?;
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

#[cfg(test)]
mod tests {
    use super::Scheduler;
    use crate::Error;
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use sqlx::PgPool;
    use std::time::Duration;

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    struct ScheduledTask {
        value: i32,
    }

    impl Scheduler for ScheduledTask {}

    #[derive(Debug)]
    struct UnserializableTask;

    impl serde::Serialize for UnserializableTask {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("can't serialize scheduled task"))
        }
    }

    impl<'de> serde::Deserialize<'de> for UnserializableTask {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            <() as serde::Deserialize>::deserialize(deserializer)?;
            Ok(Self)
        }
    }

    impl Scheduler for UnserializableTask {}

    struct TaskRow {
        step: String,
        wakeup_at: DateTime<Utc>,
    }

    async fn fetch_task_row(pool: &PgPool, id: sqlx::types::Uuid) -> TaskRow {
        let row = sqlx::query!("SELECT step, wakeup_at FROM pg_task WHERE id = $1", id)
            .fetch_one(pool)
            .await
            .unwrap();

        TaskRow {
            step: row.step,
            wakeup_at: row.wakeup_at,
        }
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
    async fn schedule_persists_the_serialized_task_and_requested_time(pool: PgPool) {
        let task = ScheduledTask { value: 7 };
        let at = Utc::now() + ChronoDuration::seconds(5);

        let id = crate::schedule(&pool, &task, at).await.unwrap();

        let row = fetch_task_row(&pool, id).await;
        assert_eq!(row.step, serde_json::to_string(&task).unwrap());
        assert!(
            (row.wakeup_at - at)
                .num_microseconds()
                .is_some_and(|diff| diff.abs() <= 1),
            "scheduled time {0:?} should match {1:?}",
            row.wakeup_at,
            at,
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn delay_schedules_tasks_after_the_requested_duration(pool: PgPool) {
        let task = ScheduledTask { value: 8 };
        let delay = Duration::from_millis(250);
        let started_at = Utc::now();

        let id = crate::delay(&pool, &task, delay).await.unwrap();

        let finished_at = Utc::now();
        let row = fetch_task_row(&pool, id).await;
        let delay = ChronoDuration::from_std(delay).unwrap();
        assert_eq!(row.step, serde_json::to_string(&task).unwrap());
        assert_timestamp_between(
            row.wakeup_at,
            started_at + delay,
            finished_at + delay + ChronoDuration::seconds(1),
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn enqueue_schedules_tasks_immediately(pool: PgPool) {
        let task = ScheduledTask { value: 9 };
        let started_at = Utc::now();

        let id = crate::enqueue(&pool, &task).await.unwrap();

        let finished_at = Utc::now();
        let row = fetch_task_row(&pool, id).await;
        assert_eq!(row.step, serde_json::to_string(&task).unwrap());
        assert_timestamp_between(
            row.wakeup_at,
            started_at,
            finished_at + ChronoDuration::seconds(1),
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn schedule_returns_serialization_errors_before_touching_the_database(pool: PgPool) {
        let err = crate::schedule(&pool, &UnserializableTask, Utc::now())
            .await
            .unwrap_err();

        assert!(matches!(err, Error::SerializeStep(_, _)));

        let row_count = sqlx::query!("SELECT id FROM pg_task")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(row_count.len(), 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn schedule_returns_add_task_errors_for_insert_failures(pool: PgPool) {
        sqlx::query!("ALTER TABLE pg_task RENAME COLUMN step TO task_step")
            .execute(&pool)
            .await
            .unwrap();

        let err = crate::schedule(&pool, &ScheduledTask { value: 10 }, Utc::now())
            .await
            .unwrap_err();

        assert!(matches!(err, Error::AddTask(sqlx::Error::Database(_))));
    }
}
