use super::{Task, WorkerLease};
use crate::{NextStep, Step};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sqlx::PgPool;
use std::{io, time::Duration};
use uuid::Uuid;

fn init_tracing() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .without_time()
            .try_init();
    });
}

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
    locked_by: Option<Uuid>,
    lock_expires_at: Option<DateTime<Utc>>,
    error: Option<String>,
}

fn serialized_step(step: &TestTask) -> String {
    serde_json::to_string(step).unwrap()
}

fn task_with_step(id: Uuid, step: &TestTask, tried: i32) -> Task {
    Task {
        id,
        step: serialized_step(step),
        tried,
    }
}

fn raw_task(id: Uuid, step: &str, tried: i32) -> Task {
    Task {
        id,
        step: step.into(),
        tried,
    }
}

fn assert_database_error(err: crate::Error) {
    assert!(matches!(err, crate::Error::Db(sqlx::Error::Database(_), _)));
}

fn worker_id() -> Uuid {
    Uuid::from_u128(1)
}

fn other_worker_id() -> Uuid {
    Uuid::from_u128(2)
}

fn worker_lease() -> WorkerLease {
    WorkerLease::new(worker_id(), Duration::from_secs(60))
}

#[test]
fn worker_lease_converts_timeout_to_microsecond_interval() {
    let lease = WorkerLease::new(worker_id(), Duration::from_micros(42));
    assert_eq!(lease.timeout.microseconds, 42);
}

#[test]
fn worker_lease_rounds_timeout_up_to_microseconds() {
    let lease = WorkerLease::new(worker_id(), Duration::from_nanos(1));
    assert_eq!(lease.timeout.microseconds, 1);
}

#[test]
fn worker_lease_saturates_large_timeouts() {
    let lease = WorkerLease::new(worker_id(), Duration::MAX);
    assert_eq!(lease.timeout.microseconds, i64::MAX);
}

async fn insert_task_row(
    pool: &PgPool,
    step: &str,
    wakeup_at: DateTime<Utc>,
    tried: i32,
    is_leased: bool,
    error: Option<&str>,
) -> Uuid {
    let (locked_by, lock_expires_at) = if is_leased {
        (
            Some(other_worker_id()),
            Some(Utc::now() + ChronoDuration::seconds(60)),
        )
    } else {
        (None, None)
    };
    sqlx::query!(
        "
            INSERT INTO pg_task (step, wakeup_at, tried, locked_by, lock_expires_at, error)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            ",
        step,
        wakeup_at,
        tried,
        locked_by,
        lock_expires_at,
        error,
    )
    .fetch_one(pool)
    .await
    .unwrap()
    .id
}

async fn insert_task(pool: &PgPool, step: &TestTask, tried: i32, is_leased: bool) -> Uuid {
    let step = serialized_step(step);
    insert_task_row(
        pool,
        &step,
        Utc::now() - ChronoDuration::milliseconds(1),
        tried,
        is_leased,
        None,
    )
    .await
}

async fn set_task_lease(pool: &PgPool, id: Uuid, worker_id: Uuid, lock_expires_at: DateTime<Utc>) {
    sqlx::query!(
        r#"
            UPDATE pg_task
            SET locked_by = $2,
                lock_expires_at = $3
            WHERE id = $1
            "#,
        id,
        worker_id,
        lock_expires_at,
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn claim_task(pool: &PgPool, step: TestTask, tried: i32) -> (Task, TestTask, WorkerLease) {
    let id = insert_task(pool, &step, tried, false).await;
    let mut tx = pool.begin().await.unwrap();
    let task = Task::fetch_ready(&mut tx).await.unwrap().unwrap();
    assert_eq!(task.id, id);
    let lease = worker_lease();
    let claimed = task
        .claim::<TestTask>(&mut tx, lease)
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();
    (task, claimed, lease)
}

async fn fetch_task_row(pool: &PgPool, id: Uuid) -> Option<TaskRow> {
    sqlx::query!(
        "
            SELECT step, wakeup_at, tried, locked_by, lock_expires_at, error
            FROM pg_task
            WHERE id = $1
            ",
        id,
    )
    .fetch_optional(pool)
    .await
    .unwrap()
    .map(|row| TaskRow {
        step: row.step,
        wakeup_at: row.wakeup_at,
        tried: row.tried,
        locked_by: row.locked_by,
        lock_expires_at: row.lock_expires_at,
        error: row.error,
    })
}

fn assert_timestamp_between(actual: DateTime<Utc>, earliest: DateTime<Utc>, latest: DateTime<Utc>) {
    assert!(
        actual >= earliest,
        "timestamp {actual:?} should be after {earliest:?}",
    );
    assert!(
        actual <= latest,
        "timestamp {actual:?} should be before {latest:?}",
    );
}

#[test]
fn delay_until_returns_none_for_ready_times() {
    assert!(Task::delay_until(Utc::now() - ChronoDuration::milliseconds(1)).is_none());
}

#[test]
fn delay_until_returns_duration_for_future_times() {
    let delay = Task::delay_until(Utc::now() + ChronoDuration::milliseconds(250)).unwrap();

    assert!(delay <= Duration::from_millis(250));
    assert!(delay > Duration::ZERO);
}

#[sqlx::test(migrations = "./migrations")]
async fn claim_marks_invalid_steps_errored(pool: PgPool) {
    sqlx::query!(
        "INSERT INTO pg_task (step, wakeup_at) VALUES ($1, $2)",
        "not-json",
        Utc::now(),
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let task = Task::fetch_ready(&mut tx).await.unwrap().unwrap();

    assert!(task
        .claim::<TestTask>(&mut tx, worker_lease())
        .await
        .unwrap()
        .is_none());

    tx.commit().await.unwrap();

    let row = sqlx::query!("SELECT tried, locked_by, lock_expires_at, error FROM pg_task LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(row.tried, 0);
    assert!(row.locked_by.is_none());
    assert!(row.lock_expires_at.is_none());
    assert!(row.error.is_some());
}

#[test]
fn unserializable_deserializes_from_unit() {
    serde_json::from_str::<Unserializable>("null").unwrap();
}

#[tokio::test]
async fn followup_step_returns_none() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres:///pg_task")
        .unwrap();

    assert!(matches!(
        crate::Step::<TestTask>::step(TestTask::Followup(Followup { value: 7 }), &pool)
            .await
            .unwrap(),
        NextStep::None
    ));
}

#[tokio::test]
#[should_panic(expected = "the test never executes the unserializable step")]
async fn unserializable_step_panics_if_executed() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres:///pg_task")
        .unwrap();

    let _ = crate::Step::<TestTask>::step(TestTask::Unserializable(Unserializable), &pool).await;
}

#[sqlx::test(migrations = "./migrations")]
async fn fetch_ready_returns_db_errors_for_query_failures(pool: PgPool) {
    insert_task(&pool, &TestTask::Valid(Valid), 0, false).await;
    sqlx::query!("ALTER TABLE pg_task RENAME COLUMN step TO task_step")
        .execute(&pool)
        .await
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = Task::fetch_ready(&mut tx).await.unwrap_err();

    assert_database_error(err);
}

#[sqlx::test(migrations = "./migrations")]
async fn mark_running_returns_db_errors_for_update_failures(pool: PgPool) {
    let id = insert_task(&pool, &TestTask::Valid(Valid), 0, false).await;
    let task = task_with_step(id, &TestTask::Valid(Valid), 0);
    sqlx::query!("ALTER TABLE pg_task RENAME COLUMN locked_by TO task_locked_by")
        .execute(&pool)
        .await
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = task
        .mark_running(&mut tx, worker_lease())
        .await
        .unwrap_err();

    assert_database_error(err);
}

#[sqlx::test(migrations = "./migrations")]
async fn claim_returns_db_errors_when_saving_deserialization_failures_fails(pool: PgPool) {
    let id = insert_task_row(&pool, "not-json", Utc::now(), 0, false, None).await;
    let task = raw_task(id, "not-json", 0);
    sqlx::query!("ALTER TABLE pg_task RENAME COLUMN error TO task_error")
        .execute(&pool)
        .await
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = task
        .claim::<TestTask>(&mut tx, worker_lease())
        .await
        .unwrap_err();

    assert_database_error(err);
}

#[sqlx::test(migrations = "./migrations")]
async fn claim_marks_valid_steps_leased(pool: PgPool) {
    let id = insert_task(&pool, &TestTask::Valid(Valid), 0, false).await;

    let started_at = Utc::now();
    let mut tx = pool.begin().await.unwrap();
    let task = Task::fetch_ready(&mut tx).await.unwrap().unwrap();
    let claimed = task
        .claim::<TestTask>(&mut tx, worker_lease())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let finished_at = Utc::now();

    assert!(matches!(claimed, Some(TestTask::Valid(Valid))));

    let row = fetch_task_row(&pool, id).await.unwrap();
    assert_eq!(row.step, serialized_step(&TestTask::Valid(Valid)));
    assert_eq!(row.tried, 0);
    assert_eq!(row.locked_by, Some(worker_id()));
    assert_timestamp_between(
        row.lock_expires_at.unwrap(),
        started_at + ChronoDuration::seconds(60),
        finished_at + ChronoDuration::seconds(61),
    );
    assert!(row.error.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn task_lease_columns_must_be_set_together(pool: PgPool) {
    let valid = serialized_step(&TestTask::Valid(Valid));
    let now = Utc::now();

    let err = sqlx::query!(
        "
            INSERT INTO pg_task (step, wakeup_at, locked_by)
            VALUES ($1, $2, $3)
            ",
        &valid,
        now,
        worker_id(),
    )
    .execute(&pool)
    .await
    .unwrap_err();
    assert!(matches!(err, sqlx::Error::Database(_)));

    let err = sqlx::query!(
        "
            INSERT INTO pg_task (step, wakeup_at, lock_expires_at)
            VALUES ($1, $2, $3)
            ",
        &valid,
        now,
        now,
    )
    .execute(&pool)
    .await
    .unwrap_err();
    assert!(matches!(err, sqlx::Error::Database(_)));
}

#[sqlx::test(migrations = "./migrations")]
async fn renew_leases_extends_only_live_owned_leases(pool: PgPool) {
    let now = Utc::now();
    let valid = serialized_step(&TestTask::Valid(Valid));
    let owned = insert_task_row(
        &pool,
        &valid,
        now - ChronoDuration::seconds(1),
        0,
        false,
        None,
    )
    .await;
    let owned_expires_at = now + ChronoDuration::seconds(30);
    set_task_lease(&pool, owned, worker_id(), owned_expires_at).await;
    let expired = insert_task_row(
        &pool,
        &valid,
        now - ChronoDuration::seconds(1),
        0,
        false,
        None,
    )
    .await;
    let expired_expires_at = now - ChronoDuration::seconds(1);
    set_task_lease(&pool, expired, worker_id(), expired_expires_at).await;
    let other_worker = insert_task_row(
        &pool,
        &valid,
        now - ChronoDuration::seconds(1),
        0,
        false,
        None,
    )
    .await;
    let other_worker_expires_at = now + ChronoDuration::seconds(30);
    set_task_lease(
        &pool,
        other_worker,
        other_worker_id(),
        other_worker_expires_at,
    )
    .await;

    let started_at = Utc::now();
    let renewed = Task::renew_leases(&pool, worker_lease(), &[owned, expired, other_worker])
        .await
        .unwrap();
    let finished_at = Utc::now();

    assert_eq!(renewed, vec![owned]);
    let owned = fetch_task_row(&pool, owned).await.unwrap();
    assert_timestamp_between(
        owned.lock_expires_at.unwrap(),
        started_at + ChronoDuration::seconds(60),
        finished_at + ChronoDuration::seconds(61),
    );
    let expired = fetch_task_row(&pool, expired).await.unwrap();
    assert!(expired.lock_expires_at.unwrap() < started_at);
    let other_worker = fetch_task_row(&pool, other_worker).await.unwrap();
    assert!(other_worker.lock_expires_at.unwrap() < started_at + ChronoDuration::seconds(45));
}

#[sqlx::test(migrations = "./migrations")]
async fn renew_leases_returns_zero_when_no_live_owned_leases_exist(pool: PgPool) {
    let now = Utc::now();
    let valid = serialized_step(&TestTask::Valid(Valid));
    let expired = insert_task_row(
        &pool,
        &valid,
        now - ChronoDuration::seconds(1),
        0,
        false,
        None,
    )
    .await;
    set_task_lease(
        &pool,
        expired,
        worker_id(),
        now - ChronoDuration::seconds(1),
    )
    .await;
    let other_worker = insert_task_row(
        &pool,
        &valid,
        now - ChronoDuration::seconds(1),
        0,
        false,
        None,
    )
    .await;
    set_task_lease(
        &pool,
        other_worker,
        other_worker_id(),
        now + ChronoDuration::seconds(30),
    )
    .await;

    assert!(
        Task::renew_leases(&pool, worker_lease(), &[expired, other_worker])
            .await
            .unwrap()
            .is_empty()
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn fetch_ready_ignores_leased_errored_and_future_tasks_and_picks_the_earliest_ready_one(
    pool: PgPool,
) {
    let now = Utc::now();
    let valid = serialized_step(&TestTask::Valid(Valid));

    let live_lease = insert_task_row(
        &pool,
        &valid,
        now - ChronoDuration::seconds(3),
        0,
        true,
        None,
    )
    .await;
    set_task_lease(
        &pool,
        live_lease,
        other_worker_id(),
        now + ChronoDuration::seconds(60),
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
    let task = Task::fetch_ready(&mut tx).await.unwrap().unwrap();
    tx.commit().await.unwrap();

    assert_eq!(task.id, expected);
    assert_eq!(task.step, valid);
    assert_eq!(task.tried, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn fetch_ready_returns_expired_leased_tasks(pool: PgPool) {
    let now = Utc::now();
    let valid = serialized_step(&TestTask::Valid(Valid));
    let expected = insert_task_row(
        &pool,
        &valid,
        now - ChronoDuration::seconds(1),
        0,
        true,
        None,
    )
    .await;
    set_task_lease(
        &pool,
        expected,
        other_worker_id(),
        now - ChronoDuration::seconds(1),
    )
    .await;

    let mut tx = pool.begin().await.unwrap();
    let task = Task::fetch_ready(&mut tx).await.unwrap().unwrap();
    tx.commit().await.unwrap();

    assert_eq!(task.id, expected);
}

#[sqlx::test(migrations = "./migrations")]
async fn fetch_ready_returns_none_when_no_tasks_are_ready(pool: PgPool) {
    let now = Utc::now();
    let valid = serialized_step(&TestTask::Valid(Valid));
    insert_task_row(
        &pool,
        &valid,
        now + ChronoDuration::seconds(1),
        0,
        false,
        None,
    )
    .await;
    insert_task_row(
        &pool,
        &valid,
        now - ChronoDuration::seconds(1),
        0,
        false,
        Some("boom"),
    )
    .await;

    let mut tx = pool.begin().await.unwrap();
    assert!(Task::fetch_ready(&mut tx).await.unwrap().is_none());
    tx.commit().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn fetch_next_available_at_returns_none_when_no_tasks_are_visible(pool: PgPool) {
    let mut tx = pool.begin().await.unwrap();
    assert!(Task::fetch_next_available_at(&mut tx)
        .await
        .unwrap()
        .is_none());
    tx.commit().await.unwrap();

    insert_task_row(
        &pool,
        &serialized_step(&TestTask::Valid(Valid)),
        Utc::now() - ChronoDuration::seconds(1),
        0,
        false,
        Some("boom"),
    )
    .await;

    let mut tx = pool.begin().await.unwrap();
    assert!(Task::fetch_next_available_at(&mut tx)
        .await
        .unwrap()
        .is_none());
    tx.commit().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn fetch_next_available_at_returns_the_earliest_visible_eligible_task(pool: PgPool) {
    let now = Utc::now();
    let valid = serialized_step(&TestTask::Valid(Valid));
    let live_lease = insert_task_row(
        &pool,
        &valid,
        now - ChronoDuration::seconds(3),
        0,
        true,
        None,
    )
    .await;
    set_task_lease(
        &pool,
        live_lease,
        other_worker_id(),
        now + ChronoDuration::seconds(2),
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
    insert_task_row(
        &pool,
        &valid,
        now + ChronoDuration::seconds(2),
        0,
        false,
        None,
    )
    .await;
    let expected = insert_task_row(
        &pool,
        &valid,
        now + ChronoDuration::seconds(1),
        0,
        false,
        None,
    )
    .await;

    let mut tx = pool.begin().await.unwrap();
    let wakeup_at = Task::fetch_next_available_at(&mut tx)
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();

    let row = fetch_task_row(&pool, expected).await.unwrap();
    assert_eq!(wakeup_at, row.wakeup_at);
}

#[sqlx::test(migrations = "./migrations")]
async fn fetch_next_available_at_returns_lease_expiry_for_ready_leased_tasks(pool: PgPool) {
    let now = Utc::now();
    let valid = serialized_step(&TestTask::Valid(Valid));
    let expected = insert_task_row(
        &pool,
        &valid,
        now - ChronoDuration::seconds(1),
        0,
        true,
        None,
    )
    .await;
    set_task_lease(
        &pool,
        expected,
        other_worker_id(),
        now + ChronoDuration::seconds(5),
    )
    .await;

    let mut tx = pool.begin().await.unwrap();
    let wakeup_at = Task::fetch_next_available_at(&mut tx)
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();

    let row = fetch_task_row(&pool, expected).await.unwrap();
    assert_eq!(wakeup_at, row.lock_expires_at.unwrap());
}

#[sqlx::test(migrations = "./migrations")]
async fn fetch_next_available_at_keeps_future_leased_tasks_delayed_until_wakeup(pool: PgPool) {
    let now = Utc::now();
    let valid = serialized_step(&TestTask::Valid(Valid));
    let expected = insert_task_row(
        &pool,
        &valid,
        now + ChronoDuration::seconds(5),
        0,
        true,
        None,
    )
    .await;
    set_task_lease(
        &pool,
        expected,
        other_worker_id(),
        now + ChronoDuration::seconds(1),
    )
    .await;

    let mut tx = pool.begin().await.unwrap();
    let wakeup_at = Task::fetch_next_available_at(&mut tx)
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();

    let row = fetch_task_row(&pool, expected).await.unwrap();
    assert_eq!(wakeup_at, row.wakeup_at);
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_completes_tasks(pool: PgPool) {
    init_tracing();
    let (task, step, lease) = claim_task(&pool, TestTask::Valid(Valid), 0).await;

    task.run_step(&pool, step, lease).await.unwrap();

    assert!(fetch_task_row(&pool, task.id).await.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_does_not_complete_tasks_after_losing_the_lease(pool: PgPool) {
    init_tracing();
    let (task, step, lease) = claim_task(&pool, TestTask::Valid(Valid), 0).await;
    set_task_lease(
        &pool,
        task.id,
        other_worker_id(),
        Utc::now() + ChronoDuration::seconds(60),
    )
    .await;

    task.run_step(&pool, step, lease).await.unwrap();

    let row = fetch_task_row(&pool, task.id).await.unwrap();
    assert_eq!(row.locked_by, Some(other_worker_id()));
    assert!(row.error.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_does_not_complete_tasks_after_the_lease_expires(pool: PgPool) {
    init_tracing();
    let (task, step, lease) = claim_task(&pool, TestTask::Valid(Valid), 0).await;
    set_task_lease(
        &pool,
        task.id,
        worker_id(),
        Utc::now() - ChronoDuration::seconds(1),
    )
    .await;

    task.run_step(&pool, step, lease).await.unwrap();

    let row = fetch_task_row(&pool, task.id).await.unwrap();
    assert_eq!(row.locked_by, Some(worker_id()));
    assert!(row.lock_expires_at.unwrap() < Utc::now());
    assert!(row.error.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_returns_db_errors_when_completing_tasks_fails(pool: PgPool) {
    let step = TestTask::Valid(Valid);
    let id = insert_task(&pool, &step, 0, false).await;
    let task = task_with_step(id, &step, 0);
    sqlx::query!("ALTER TABLE pg_task RENAME COLUMN id TO task_id")
        .execute(&pool)
        .await
        .unwrap();

    let err = task
        .run_step(&pool, step, worker_lease())
        .await
        .unwrap_err();

    assert_database_error(err);
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_saves_immediate_next_step_and_resets_retries(pool: PgPool) {
    let (task, step, lease) =
        claim_task(&pool, TestTask::AdvanceNow(AdvanceNow { value: 41 }), 2).await;
    let started_at = Utc::now();

    task.run_step(&pool, step, lease).await.unwrap();

    let finished_at = Utc::now();
    let row = fetch_task_row(&pool, task.id).await.unwrap();
    assert_eq!(
        row.step,
        serialized_step(&TestTask::Followup(Followup { value: 42 })),
    );
    assert_eq!(row.tried, 0);
    assert!(row.locked_by.is_none());
    assert!(row.lock_expires_at.is_none());
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
    let (task, step, lease) = claim_task(
        &pool,
        TestTask::AdvanceLater(AdvanceLater {
            value: 9,
            delay_ms: delay.as_millis() as u64,
        }),
        3,
    )
    .await;
    let started_at = Utc::now();

    task.run_step(&pool, step, lease).await.unwrap();

    let finished_at = Utc::now();
    let row = fetch_task_row(&pool, task.id).await.unwrap();
    let delay = ChronoDuration::from_std(delay).unwrap();
    assert_eq!(
        row.step,
        serialized_step(&TestTask::Followup(Followup { value: 10 })),
    );
    assert_eq!(row.tried, 0);
    assert!(row.locked_by.is_none());
    assert!(row.lock_expires_at.is_none());
    assert!(row.error.is_none());
    assert_timestamp_between(
        row.wakeup_at,
        started_at + delay,
        finished_at + delay + ChronoDuration::seconds(1),
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_does_not_save_next_step_after_losing_the_lease(pool: PgPool) {
    init_tracing();
    let (task, step, lease) =
        claim_task(&pool, TestTask::AdvanceNow(AdvanceNow { value: 41 }), 2).await;
    set_task_lease(
        &pool,
        task.id,
        other_worker_id(),
        Utc::now() + ChronoDuration::seconds(60),
    )
    .await;

    task.run_step(&pool, step, lease).await.unwrap();

    let row = fetch_task_row(&pool, task.id).await.unwrap();
    assert_eq!(
        row.step,
        serialized_step(&TestTask::AdvanceNow(AdvanceNow { value: 41 })),
    );
    assert_eq!(row.tried, 2);
    assert_eq!(row.locked_by, Some(other_worker_id()));
    assert!(row.error.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_returns_db_errors_when_saving_next_step_fails(pool: PgPool) {
    let step = TestTask::AdvanceNow(AdvanceNow { value: 41 });
    let id = insert_task(&pool, &step, 2, false).await;
    let task = task_with_step(id, &step, 2);
    sqlx::query!("ALTER TABLE pg_task RENAME COLUMN step TO task_step")
        .execute(&pool)
        .await
        .unwrap();

    let err = task
        .run_step(&pool, step, worker_lease())
        .await
        .unwrap_err();

    assert_database_error(err);
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_schedules_retries_before_the_retry_limit(pool: PgPool) {
    init_tracing();
    let retry_delay = <RetryFail as Step<TestTask>>::RETRY_DELAY;
    let (task, step, lease) = claim_task(&pool, TestTask::RetryFail(RetryFail), 1).await;
    let started_at = Utc::now();

    task.run_step(&pool, step, lease).await.unwrap();

    let finished_at = Utc::now();
    let row = fetch_task_row(&pool, task.id).await.unwrap();
    let retry_delay = ChronoDuration::from_std(retry_delay).unwrap();
    assert_eq!(row.step, serialized_step(&TestTask::RetryFail(RetryFail)));
    assert_eq!(row.tried, 2);
    assert!(row.locked_by.is_none());
    assert!(row.lock_expires_at.is_none());
    assert!(row.error.is_none());
    assert_timestamp_between(
        row.wakeup_at,
        started_at + retry_delay,
        finished_at + retry_delay + ChronoDuration::seconds(1),
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_does_not_schedule_retries_after_losing_the_lease(pool: PgPool) {
    init_tracing();
    let (task, step, lease) = claim_task(&pool, TestTask::RetryFail(RetryFail), 1).await;
    set_task_lease(
        &pool,
        task.id,
        other_worker_id(),
        Utc::now() + ChronoDuration::seconds(60),
    )
    .await;

    task.run_step(&pool, step, lease).await.unwrap();

    let row = fetch_task_row(&pool, task.id).await.unwrap();
    assert_eq!(row.step, serialized_step(&TestTask::RetryFail(RetryFail)));
    assert_eq!(row.tried, 1);
    assert_eq!(row.locked_by, Some(other_worker_id()));
    assert!(row.error.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_returns_db_errors_when_retrying_fails(pool: PgPool) {
    let step = TestTask::RetryFail(RetryFail);
    let id = insert_task(&pool, &step, 1, false).await;
    let task = task_with_step(id, &step, 1);
    sqlx::query!("ALTER TABLE pg_task RENAME COLUMN wakeup_at TO scheduled_at")
        .execute(&pool)
        .await
        .unwrap();

    let err = task
        .run_step(&pool, step, worker_lease())
        .await
        .unwrap_err();

    assert_database_error(err);
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_saves_terminal_errors_after_retry_limit(pool: PgPool) {
    init_tracing();
    let retry_limit = <RetryFail as Step<TestTask>>::RETRY_LIMIT;
    let (task, step, lease) = claim_task(&pool, TestTask::RetryFail(RetryFail), retry_limit).await;
    let started_at = Utc::now();

    task.run_step(&pool, step, lease).await.unwrap();

    let finished_at = Utc::now();
    let row = fetch_task_row(&pool, task.id).await.unwrap();
    assert_eq!(row.step, serialized_step(&TestTask::RetryFail(RetryFail)));
    assert_eq!(row.tried, retry_limit + 1);
    assert!(row.locked_by.is_none());
    assert!(row.lock_expires_at.is_none());
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
async fn run_step_does_not_save_terminal_errors_after_losing_the_lease(pool: PgPool) {
    init_tracing();
    let retry_limit = <RetryFail as Step<TestTask>>::RETRY_LIMIT;
    let (task, step, lease) = claim_task(&pool, TestTask::RetryFail(RetryFail), retry_limit).await;
    set_task_lease(
        &pool,
        task.id,
        other_worker_id(),
        Utc::now() + ChronoDuration::seconds(60),
    )
    .await;

    task.run_step(&pool, step, lease).await.unwrap();

    let row = fetch_task_row(&pool, task.id).await.unwrap();
    assert_eq!(row.step, serialized_step(&TestTask::RetryFail(RetryFail)));
    assert_eq!(row.tried, retry_limit);
    assert_eq!(row.locked_by, Some(other_worker_id()));
    assert!(row.error.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_returns_db_errors_when_saving_terminal_errors_fails(pool: PgPool) {
    let step = TestTask::RetryFail(RetryFail);
    let retry_limit = <RetryFail as Step<TestTask>>::RETRY_LIMIT;
    let id = insert_task(&pool, &step, retry_limit, false).await;
    let task = task_with_step(id, &step, retry_limit);
    sqlx::query!("ALTER TABLE pg_task RENAME COLUMN error TO task_error")
        .execute(&pool)
        .await
        .unwrap();

    let err = task
        .run_step(&pool, step, worker_lease())
        .await
        .unwrap_err();

    assert_database_error(err);
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_saves_next_step_serialization_failures_as_errors(pool: PgPool) {
    let (task, step, lease) = claim_task(&pool, TestTask::BrokenNext(BrokenNext), 0).await;

    task.run_step(&pool, step, lease).await.unwrap();

    let row = fetch_task_row(&pool, task.id).await.unwrap();
    assert_eq!(row.step, serialized_step(&TestTask::BrokenNext(BrokenNext)),);
    assert_eq!(row.tried, 1);
    assert!(row.locked_by.is_none());
    assert!(row.lock_expires_at.is_none());
    assert!(row
        .error
        .as_deref()
        .is_some_and(|error| error.contains("can't serialize test step")));
}

#[sqlx::test(migrations = "./migrations")]
async fn run_step_does_not_save_next_step_serialization_errors_after_losing_the_lease(
    pool: PgPool,
) {
    init_tracing();
    let (task, step, lease) = claim_task(&pool, TestTask::BrokenNext(BrokenNext), 0).await;
    set_task_lease(
        &pool,
        task.id,
        other_worker_id(),
        Utc::now() + ChronoDuration::seconds(60),
    )
    .await;

    task.run_step(&pool, step, lease).await.unwrap();

    let row = fetch_task_row(&pool, task.id).await.unwrap();
    assert_eq!(row.step, serialized_step(&TestTask::BrokenNext(BrokenNext)),);
    assert_eq!(row.tried, 0);
    assert_eq!(row.locked_by, Some(other_worker_id()));
    assert!(row.error.is_none());
}
