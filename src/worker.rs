use crate::{
    listener::Listener,
    task::Task,
    util::{db_error, is_connection_error, is_pool_timeout, wait_for_reconnection},
    Error, Result, Step, LOST_CONNECTION_SLEEP,
};
use sqlx::postgres::PgPool;
use std::{marker::PhantomData, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, time::sleep};
use tracing::{error, info, trace, warn};

const LOCKED_TASK_RECHECK_DELAY: Duration = Duration::from_millis(100);

/// A worker for processing tasks
pub struct Worker<T> {
    db: PgPool,
    listener: Listener,
    tasks: PhantomData<T>,
    concurrency: NonZeroUsize,
}

impl<S: Step<S> + 'static> Worker<S> {
    /// Creates a new worker
    pub fn new(db: PgPool) -> Self {
        let listener = Listener::new();
        let concurrency = NonZeroUsize::new(num_cpus::get()).unwrap_or(NonZeroUsize::MIN);
        Self {
            db,
            listener,
            concurrency,
            tasks: PhantomData,
        }
    }

    /// Sets the number of concurrent tasks, default is the number of CPU cores
    pub fn with_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Runs all ready tasks to completion and waits for new ones
    pub async fn run(&self) -> Result<()> {
        self.listener.listen(self.db.clone()).await?;

        let semaphore = Arc::new(Semaphore::new(self.concurrency.get()));

        let result = loop {
            match self.recv_task().await {
                Ok(Some((task, step))) => {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(Error::UnreachableWorkerSemaphoreClosed)?;
                    let db = self.db.clone();
                    tokio::spawn(async move {
                        if let Err(e) = task.run_step(&db, step).await {
                            error!("[{}] {}", task.id, source_chain::to_string(&e));
                        };
                        drop(permit);
                    });
                }
                Ok(None) => {
                    break Ok(());
                }
                Err(e) => {
                    if let Err(error) = self.handle_recv_task_error(e).await {
                        break Err(error);
                    }
                }
            }
        };
        self.finish_run(result, semaphore).await
    }

    /// Waits until the next task is ready, marks it running and returns it.
    /// Returns `None` if the worker is stopped
    async fn recv_task(&self) -> Result<Option<(Task, S)>> {
        trace!("Receiving the next task");

        loop {
            if self.listener.time_to_stop_worker() {
                return Ok(None);
            }

            if let Some(error) = self.listener.take_error() {
                return Err(error);
            }

            let table_changes = self.listener.subscribe();
            let mut tx = self.db.begin().await.map_err(db_error!("begin"))?;

            let Some(task) = Task::fetch_ready(&mut tx).await? else {
                let next_wakeup_at = Task::fetch_next_wakeup_at(&mut tx).await?;
                tx.commit().await.map_err(db_error!("no ready tasks"))?;

                if let Some(wakeup_at) = next_wakeup_at {
                    let delay = Task::delay_until(wakeup_at).unwrap_or(LOCKED_TASK_RECHECK_DELAY);
                    table_changes.wait_for(delay).await;
                } else {
                    table_changes.wait_forever().await;
                }
                continue;
            };

            let Some(step) = task.claim(&mut tx).await? else {
                tx.commit().await.map_err(db_error!("save error"))?;
                continue;
            };
            tx.commit().await.map_err(db_error!("mark running"))?;
            return Ok(Some((task, step)));
        }
    }

    async fn handle_recv_task_error(&self, error: Error) -> Result<()> {
        if matches!(&error, Error::Db(db_error, _) if is_connection_error(db_error)) {
            warn!(
                "Task fetching stopped because the database connection was interrupted:\n{}",
                source_chain::to_string(&error)
            );
            sleep(LOST_CONNECTION_SLEEP).await;
            wait_for_reconnection(&self.db, LOST_CONNECTION_SLEEP).await?;
            warn!("Task fetching resumed");
            Ok(())
        } else if matches!(&error, Error::Db(db_error, _) if is_pool_timeout(db_error)) {
            warn!(
                "Task fetching is waiting for a free database connection from the pool:\n{}",
                source_chain::to_string(&error)
            );
            sleep(LOST_CONNECTION_SLEEP).await;
            Ok(())
        } else {
            Err(error)
        }
    }

    async fn finish_run(&self, result: Result<()>, semaphore: Arc<Semaphore>) -> Result<()> {
        self.listener.shutdown();
        // Drain in-flight steps before returning so a restarted worker can't
        // reclaim them as stale while they are still running.
        self.wait_for_steps_to_finish(semaphore).await;
        if result.is_ok() {
            info!("Stopped");
        }
        result
    }

    async fn wait_for_steps_to_finish(&self, semaphore: Arc<Semaphore>) {
        let mut logged_tasks_left = None;
        loop {
            let tasks_left = self.concurrency.get() - semaphore.available_permits();
            if tasks_left == 0 {
                break;
            }
            if let Some(logged) = logged_tasks_left {
                if logged != tasks_left {
                    trace!("Waiting for the current steps of {tasks_left} tasks to finish...");
                }
            } else {
                info!("Waiting for the current steps of {tasks_left} tasks to finish...");
            }
            logged_tasks_left = Some(tasks_left);
            sleep(Duration::from_secs_f32(0.1)).await;
        }
        if logged_tasks_left.is_some() {
            trace!("The current step of every task is done")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Worker;
    use crate::{Error, NextStep, Step};
    use chrono::{Duration as ChronoDuration, Utc};
    use sqlx::{postgres::PgPoolOptions, types::Uuid, PgPool};
    use std::{
        collections::HashMap,
        io,
        num::NonZeroUsize,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex, OnceLock,
        },
        time::Duration,
    };
    use tokio::{
        sync::{Notify, Semaphore},
        time::{sleep, timeout},
    };

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
    pub(super) struct Noop;

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct Advance {
        key: u64,
    }

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct Finish {
        key: u64,
    }

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct Complete {
        key: u64,
    }

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct Blocking {
        key: u64,
    }

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct FailSavingError {
        key: u64,
    }

    crate::task!(TestTask {
        Noop,
        Advance,
        Finish,
        Complete,
        Blocking,
        FailSavingError,
    });

    #[async_trait::async_trait]
    impl Step<TestTask> for Noop {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            Ok(NextStep::None)
        }
    }

    #[async_trait::async_trait]
    impl Step<TestTask> for Advance {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            step_state(self.key).record("advance");
            NextStep::now(Finish { key: self.key })
        }
    }

    #[async_trait::async_trait]
    impl Step<TestTask> for Finish {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            step_state(self.key).record("finish");
            NextStep::none()
        }
    }

    #[async_trait::async_trait]
    impl Step<TestTask> for Complete {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            step_state(self.key).record("complete");
            NextStep::none()
        }
    }

    #[async_trait::async_trait]
    impl Step<TestTask> for Blocking {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            let state = step_state(self.key);
            state.record("started");
            state.wait_for_release().await;
            state.record("completed");
            NextStep::none()
        }
    }

    #[async_trait::async_trait]
    impl Step<TestTask> for FailSavingError {
        async fn step(self, db: &PgPool) -> crate::StepResult<TestTask> {
            let state = step_state(self.key);
            state.record("started");
            sqlx::query!("ALTER TABLE pg_task RENAME COLUMN error TO task_error")
                .execute(db)
                .await
                .unwrap();
            state.record("save error failed");
            Err(io::Error::other("step failed").into())
        }

        fn retry_limit(&self) -> i32 {
            0
        }
    }

    struct StepState {
        events: Mutex<Vec<&'static str>>,
        events_changed: Notify,
        release: Notify,
    }

    impl StepState {
        fn new() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                events_changed: Notify::new(),
                release: Notify::new(),
            }
        }

        fn record(&self, event: &'static str) {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event);
            self.events_changed.notify_waiters();
        }

        fn release(&self) {
            self.release.notify_waiters();
        }

        fn events(&self) -> Vec<&'static str> {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        async fn wait_for_events(&self, count: usize) {
            timeout(Duration::from_secs(1), async {
                loop {
                    if self
                        .events
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .len()
                        >= count
                    {
                        return;
                    }
                    self.events_changed.notified().await;
                }
            })
            .await
            .unwrap();
        }

        async fn wait_for_release(&self) {
            self.release.notified().await;
        }
    }

    struct StepStateGuard {
        key: u64,
        state: Arc<StepState>,
    }

    impl StepStateGuard {
        fn new() -> Self {
            let key = NEXT_STEP_STATE_KEY.fetch_add(1, Ordering::Relaxed);
            let state = Arc::new(StepState::new());
            step_states()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key, state.clone());
            Self { key, state }
        }

        fn key(&self) -> u64 {
            self.key
        }

        fn state(&self) -> Arc<StepState> {
            self.state.clone()
        }
    }

    impl Drop for StepStateGuard {
        fn drop(&mut self) {
            step_states()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&self.key);
        }
    }

    static NEXT_STEP_STATE_KEY: AtomicU64 = AtomicU64::new(1);
    static STEP_STATES: OnceLock<Mutex<HashMap<u64, Arc<StepState>>>> = OnceLock::new();

    fn step_states() -> &'static Mutex<HashMap<u64, Arc<StepState>>> {
        STEP_STATES.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn step_state(key: u64) -> Arc<StepState> {
        step_states()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .cloned()
            .unwrap()
    }

    fn connection_error() -> Error {
        Error::Db(
            sqlx::Error::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "worker connection dropped",
            )),
            "test".into(),
        )
    }

    async fn insert_raw_task(
        pool: &PgPool,
        step: &str,
        wakeup_at: chrono::DateTime<Utc>,
        is_running: bool,
        error: Option<&str>,
    ) -> Uuid {
        sqlx::query!(
            "
            INSERT INTO pg_task (step, wakeup_at, is_running, error)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            ",
            step,
            wakeup_at,
            is_running,
            error,
        )
        .fetch_one(pool)
        .await
        .unwrap()
        .id
    }

    async fn insert_task_at(
        pool: &PgPool,
        step: &TestTask,
        wakeup_at: chrono::DateTime<Utc>,
        is_running: bool,
    ) -> Uuid {
        insert_raw_task(
            pool,
            &serde_json::to_string(step).unwrap(),
            wakeup_at,
            is_running,
            None,
        )
        .await
    }

    async fn insert_task(pool: &PgPool, step: &TestTask, is_running: bool) {
        insert_task_at(
            pool,
            step,
            Utc::now() - ChronoDuration::milliseconds(1),
            is_running,
        )
        .await;
    }

    async fn connect_to_current_db(
        pool: &PgPool,
        max_connections: u32,
        acquire_timeout: Duration,
    ) -> PgPool {
        let db_name: String = sqlx::query_scalar!(r#"SELECT current_database() AS "db_name!""#)
            .fetch_one(pool)
            .await
            .unwrap();

        PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(acquire_timeout)
            .connect(&format!("postgres:///{db_name}"))
            .await
            .unwrap()
    }

    async fn task_count(pool: &PgPool) -> i64 {
        sqlx::query!("SELECT id FROM pg_task")
            .fetch_all(pool)
            .await
            .unwrap()
            .len() as i64
    }

    async fn stop_worker(pool: &PgPool) {
        sqlx::query!("NOTIFY pg_task_changed, 'stop_worker'")
            .execute(pool)
            .await
            .unwrap();
    }

    fn nonzero(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn spawn_worker(pool: PgPool) -> tokio::task::JoinHandle<crate::Result<()>> {
        spawn_worker_with_concurrency(pool, 1)
    }

    fn spawn_worker_with_concurrency(
        pool: PgPool,
        concurrency: usize,
    ) -> tokio::task::JoinHandle<crate::Result<()>> {
        tokio::spawn(async move {
            Worker::<TestTask>::new(pool)
                .with_concurrency(nonzero(concurrency))
                .run()
                .await
        })
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_returns_listener_startup_errors(pool: PgPool) {
        let worker = Worker::<TestTask>::new(pool);
        worker.listener.fail_next_listen_for_tests();

        let err = worker.run().await.unwrap_err();

        assert!(matches!(
            err,
            Error::ListenerListen(sqlx::Error::Protocol(_))
        ));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn handle_recv_task_error_returns_permanent_fetch_errors(pool: PgPool) {
        sqlx::query!("ALTER TABLE pg_task RENAME COLUMN step TO task_step")
            .execute(&pool)
            .await
            .unwrap();

        let err = sqlx::query!("SELECT step FROM pg_task")
            .fetch_one(&pool)
            .await
            .unwrap_err();

        let worker = Worker::<TestTask>::new(pool);
        let err = worker
            .handle_recv_task_error(Error::Db(err, "test".into()))
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Db(sqlx::Error::Database(_), _)));
    }

    #[tokio::test]
    async fn handle_recv_task_error_retries_pool_timeouts() {
        init_tracing();
        let worker = Worker::<TestTask>::new(
            PgPoolOptions::new()
                .connect_lazy("postgres:///pg_task")
                .unwrap(),
        );

        worker
            .handle_recv_task_error(Error::Db(sqlx::Error::PoolTimedOut, "test".into()))
            .await
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn handle_recv_task_error_waits_for_reconnection_after_connection_errors(pool: PgPool) {
        init_tracing();
        let worker = Worker::<TestTask>::new(pool);

        worker
            .handle_recv_task_error(connection_error())
            .await
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn handle_recv_task_error_returns_reconnection_failures(pool: PgPool) {
        sqlx::query!("ALTER TABLE pg_task RENAME COLUMN id TO task_id")
            .execute(&pool)
            .await
            .unwrap();

        let worker = Worker::<TestTask>::new(pool);
        let err = worker
            .handle_recv_task_error(connection_error())
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Db(sqlx::Error::Database(_), _)));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn recv_task_returns_listener_errors(pool: PgPool) {
        let worker = Worker::<TestTask>::new(pool);
        worker
            .listener
            .set_error_for_tests(Error::ListenerReceive(sqlx::Error::Protocol(
                "listener failed".into(),
            )));

        let err = worker.recv_task().await.unwrap_err();

        assert!(matches!(
            err,
            Error::ListenerReceive(sqlx::Error::Protocol(_))
        ));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn recv_task_stops_even_if_listener_has_failed(pool: PgPool) {
        let worker = Worker::<TestTask>::new(pool);
        worker
            .listener
            .set_error_for_tests(Error::ListenerReceive(sqlx::Error::Protocol(
                "listener failed".into(),
            )));
        worker.listener.stop_worker_for_tests();

        assert!(worker.recv_task().await.unwrap().is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn recv_task_returns_begin_errors_when_the_pool_is_closed(pool: PgPool) {
        let worker = Worker::<TestTask>::new(pool.clone());
        pool.close().await;

        let err = worker.recv_task().await.unwrap_err();

        match err {
            Error::Db(sqlx::Error::PoolClosed, context) => {
                assert!(context.contains("begin"));
            }
            _ => panic!("expected a pool-closed begin error"),
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn recv_task_rechecks_locked_ready_tasks_without_notifications(pool: PgPool) {
        let id = insert_task_at(
            &pool,
            &TestTask::Noop(Noop),
            Utc::now() - ChronoDuration::milliseconds(1),
            false,
        )
        .await;
        let mut tx = pool.begin().await.unwrap();
        let locked = sqlx::query!("SELECT id FROM pg_task WHERE id = $1 FOR UPDATE", id)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(locked.id, id);

        let worker = Worker::<TestTask>::new(pool);
        let recv = tokio::spawn(async move { worker.recv_task().await });

        sleep(Duration::from_millis(50)).await;
        assert!(!recv.is_finished());

        tx.rollback().await.unwrap();

        let (task, step) = timeout(Duration::from_secs(1), recv)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(task.id, id);
        assert!(matches!(step, TestTask::Noop(Noop)));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn two_workers_claim_ready_tasks_once(pool: PgPool) {
        let first_id = insert_task_at(
            &pool,
            &TestTask::Noop(Noop),
            Utc::now() - ChronoDuration::milliseconds(1),
            false,
        )
        .await;
        let second_id = insert_task_at(
            &pool,
            &TestTask::Noop(Noop),
            Utc::now() - ChronoDuration::milliseconds(1),
            false,
        )
        .await;
        let first_worker = Worker::<TestTask>::new(pool.clone());
        let second_worker = Worker::<TestTask>::new(pool.clone());

        let first_recv = tokio::spawn(async move { first_worker.recv_task().await });
        let second_recv = tokio::spawn(async move { second_worker.recv_task().await });

        let (first_task, first_step) = timeout(Duration::from_secs(1), first_recv)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap();
        let (second_task, second_step) = timeout(Duration::from_secs(1), second_recv)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap();

        assert!(matches!(first_step, TestTask::Noop(Noop)));
        assert!(matches!(second_step, TestTask::Noop(Noop)));
        assert_ne!(first_task.id, second_task.id);
        assert!([first_id, second_id].contains(&first_task.id));
        assert!([first_id, second_id].contains(&second_task.id));

        let running = sqlx::query!("SELECT id FROM pg_task WHERE is_running = true")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(running.len(), 2);
    }

    #[tokio::test]
    async fn finish_run_waits_for_inflight_steps_before_returning_errors() {
        init_tracing();
        let worker = Arc::new(
            Worker::<TestTask>::new(
                PgPoolOptions::new()
                    .connect_lazy("postgres:///pg_task")
                    .unwrap(),
            )
            .with_concurrency(nonzero(1)),
        );
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        let task = tokio::spawn({
            let worker = worker.clone();
            let semaphore = semaphore.clone();
            async move {
                worker
                    .finish_run(
                        Err(Error::ListenerReceive(sqlx::Error::Protocol(
                            "listener failed".into(),
                        ))),
                        semaphore,
                    )
                    .await
            }
        });

        sleep(Duration::from_millis(50)).await;
        assert!(!task.is_finished());

        drop(permit);

        let err = task.await.unwrap().unwrap_err();
        assert!(matches!(
            err,
            Error::ListenerReceive(sqlx::Error::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn wait_for_steps_to_finish_rechecks_when_the_inflight_task_count_changes() {
        init_tracing();
        let worker = Arc::new(
            Worker::<TestTask>::new(
                PgPoolOptions::new()
                    .connect_lazy("postgres:///pg_task")
                    .unwrap(),
            )
            .with_concurrency(nonzero(2)),
        );
        let semaphore = Arc::new(Semaphore::new(2));
        let first = semaphore.clone().acquire_owned().await.unwrap();
        let second = semaphore.clone().acquire_owned().await.unwrap();

        let wait = tokio::spawn({
            let worker = worker.clone();
            let semaphore = semaphore.clone();
            async move {
                worker.wait_for_steps_to_finish(semaphore).await;
            }
        });

        sleep(Duration::from_millis(10)).await;
        drop(first);

        sleep(Duration::from_millis(150)).await;
        assert!(!wait.is_finished());

        drop(second);

        timeout(Duration::from_secs(1), wait)
            .await
            .unwrap()
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_processes_followup_steps_to_completion(pool: PgPool) {
        let state = StepStateGuard::new();
        insert_task(
            &pool,
            &TestTask::Advance(Advance { key: state.key() }),
            false,
        )
        .await;

        let worker = spawn_worker(pool.clone());

        state.state().wait_for_events(2).await;
        stop_worker(&pool).await;

        timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(state.state().events(), vec!["advance", "finish"]);
        assert_eq!(task_count(&pool).await, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_returns_listener_errors_when_the_pool_is_closed(pool: PgPool) {
        let worker = spawn_worker(pool.clone());

        sleep(Duration::from_millis(100)).await;
        pool.close().await;

        let err = timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        assert!(matches!(
            err,
            Error::ListenerReceive(sqlx::Error::PoolClosed)
        ));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_recovers_from_pool_timeouts_until_a_stop_notification_arrives(pool: PgPool) {
        let worker_pool = connect_to_current_db(&pool, 1, Duration::from_millis(20)).await;
        let worker = spawn_worker(worker_pool);

        sleep(Duration::from_millis(100)).await;
        assert!(!worker.is_finished());

        stop_worker(&pool).await;

        timeout(Duration::from_secs(3), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_wakes_up_for_tasks_inserted_while_idle(pool: PgPool) {
        let state = StepStateGuard::new();
        let worker = spawn_worker(pool.clone());

        sleep(Duration::from_millis(100)).await;
        insert_task(
            &pool,
            &TestTask::Complete(Complete { key: state.key() }),
            false,
        )
        .await;

        state.state().wait_for_events(1).await;
        stop_worker(&pool).await;

        timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(state.state().events(), vec!["complete"]);
        assert_eq!(task_count(&pool).await, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_processes_noop_tasks_to_completion(pool: PgPool) {
        insert_task(&pool, &TestTask::Noop(Noop), false).await;

        let worker = spawn_worker(pool.clone());

        timeout(Duration::from_secs(1), async {
            loop {
                if task_count(&pool).await == 0 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        stop_worker(&pool).await;

        timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_waits_for_future_tasks_to_become_ready_without_notifications(pool: PgPool) {
        let state = StepStateGuard::new();
        insert_task_at(
            &pool,
            &TestTask::Complete(Complete { key: state.key() }),
            Utc::now() + ChronoDuration::milliseconds(150),
            false,
        )
        .await;

        let worker = spawn_worker(pool.clone());

        assert!(
            timeout(Duration::from_millis(50), state.state().wait_for_events(1))
                .await
                .is_err()
        );

        state.state().wait_for_events(1).await;
        stop_worker(&pool).await;

        timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(state.state().events(), vec!["complete"]);
        assert_eq!(task_count(&pool).await, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn starting_another_worker_does_not_unlock_live_tasks(pool: PgPool) {
        let state = StepStateGuard::new();
        insert_task(
            &pool,
            &TestTask::Blocking(Blocking { key: state.key() }),
            false,
        )
        .await;

        let first_worker = spawn_worker(pool.clone());
        state.state().wait_for_events(1).await;

        let second_worker = spawn_worker(pool.clone());
        sleep(Duration::from_millis(150)).await;
        assert_eq!(state.state().events(), vec!["started"]);

        stop_worker(&pool).await;
        state.state().release();

        timeout(Duration::from_secs(1), first_worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(1), second_worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(state.state().events(), vec!["started", "completed"]);
        assert_eq!(task_count(&pool).await, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_skips_invalid_tasks_and_keeps_processing_ready_tasks(pool: PgPool) {
        let invalid_id = insert_raw_task(
            &pool,
            "not-json",
            Utc::now() - ChronoDuration::milliseconds(10),
            false,
            None,
        )
        .await;
        let state = StepStateGuard::new();
        insert_task(
            &pool,
            &TestTask::Complete(Complete { key: state.key() }),
            false,
        )
        .await;

        let worker = spawn_worker(pool.clone());

        state.state().wait_for_events(1).await;
        stop_worker(&pool).await;

        timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let invalid_row = sqlx::query!(
            "SELECT tried, is_running, error FROM pg_task WHERE id = $1",
            invalid_id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(state.state().events(), vec!["complete"]);
        assert_eq!(invalid_row.tried, 0);
        assert!(!invalid_row.is_running);
        assert!(invalid_row.error.is_some());
        assert_eq!(task_count(&pool).await, 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_stops_after_running_steps_finish(pool: PgPool) {
        let state = StepStateGuard::new();
        insert_task(
            &pool,
            &TestTask::Blocking(Blocking { key: state.key() }),
            false,
        )
        .await;

        let worker = spawn_worker(pool.clone());

        state.state().wait_for_events(1).await;
        stop_worker(&pool).await;

        sleep(Duration::from_millis(50)).await;
        assert!(!worker.is_finished());

        state.state().release();

        timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(state.state().events(), vec!["started", "completed"]);
        assert_eq!(task_count(&pool).await, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_returns_fetch_errors_after_spawned_step_persistence_errors(pool: PgPool) {
        let state = StepStateGuard::new();
        insert_task(
            &pool,
            &TestTask::FailSavingError(FailSavingError { key: state.key() }),
            false,
        )
        .await;

        let worker = spawn_worker(pool.clone());

        state.state().wait_for_events(2).await;
        let err = timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        assert_eq!(state.state().events(), vec!["started", "save error failed"]);
        assert!(matches!(err, Error::Db(sqlx::Error::Database(_), _)));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_processes_multiple_blocking_steps_up_to_the_concurrency_limit(pool: PgPool) {
        let first = StepStateGuard::new();
        let second = StepStateGuard::new();
        insert_task(
            &pool,
            &TestTask::Blocking(Blocking { key: first.key() }),
            false,
        )
        .await;
        insert_task(
            &pool,
            &TestTask::Blocking(Blocking { key: second.key() }),
            false,
        )
        .await;

        let worker = spawn_worker_with_concurrency(pool.clone(), 2);

        first.state().wait_for_events(1).await;
        second.state().wait_for_events(1).await;
        stop_worker(&pool).await;

        assert!(!worker.is_finished());

        first.state().release();
        second.state().release();

        timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(first.state().events(), vec!["started", "completed"]);
        assert_eq!(second.state().events(), vec!["started", "completed"]);
        assert_eq!(task_count(&pool).await, 0);
    }
}
