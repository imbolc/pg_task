use crate::{
    listener::Listener,
    task::Task,
    util::{db_error, is_connection_error, is_pool_timeout, wait_for_reconnection},
    Error, Result, Step, LOST_CONNECTION_SLEEP,
};
use sqlx::postgres::PgPool;
use std::{marker::PhantomData, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, time::sleep};
use tracing::{debug, error, info, trace, warn};

/// A worker for processing tasks
pub struct Worker<T> {
    db: PgPool,
    listener: Listener,
    tasks: PhantomData<T>,
    concurrency: usize,
}

impl<S: Step<S> + 'static> Worker<S> {
    /// Creates a new worker
    pub fn new(db: PgPool) -> Self {
        let listener = Listener::new();
        let concurrency = num_cpus::get();
        Self {
            db,
            listener,
            concurrency,
            tasks: PhantomData,
        }
    }

    /// Sets the number of concurrent tasks, default is the number of CPU cores
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Runs all ready tasks to completion and waits for new ones
    pub async fn run(&self) -> Result<()> {
        self.unlock_stale_tasks().await?;
        self.listener.listen(self.db.clone()).await?;

        let semaphore = Arc::new(Semaphore::new(self.concurrency));

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

    /// Unlocks all tasks. This is intended to run at the start of the worker as
    /// some tasks could remain locked as running indefinitely if the
    /// previous run ended due to some kind of crash.
    async fn unlock_stale_tasks(&self) -> Result<()> {
        let unlocked =
            sqlx::query!("UPDATE pg_task SET is_running = false WHERE is_running = true")
                .execute(&self.db)
                .await
                .map_err(Error::UnlockStaleTasks)?
                .rows_affected();
        if unlocked == 0 {
            debug!("No stale tasks to unlock")
        } else {
            debug!("Unlocked {} stale tasks", unlocked)
        }
        Ok(())
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

            let Some(task) = Task::fetch_closest(&mut tx).await? else {
                // No tasks, waiting for the tasks table changes
                tx.commit().await.map_err(db_error!("no tasks"))?;
                table_changes.wait_forever().await;
                continue;
            };

            if let Some(delay) = task.wait_before_running() {
                // Waiting until a task is ready or until the listener wakes us.
                tx.commit().await.map_err(db_error!("wait"))?;
                table_changes.wait_for(delay).await;
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
            let tasks_left = self.concurrency - semaphore.available_permits();
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

    crate::task!(TestTask {
        Noop,
        Advance,
        Finish,
        Complete,
        Blocking,
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
        insert_task_at(pool, step, Utc::now(), is_running).await;
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

    fn spawn_worker(pool: PgPool) -> tokio::task::JoinHandle<crate::Result<()>> {
        spawn_worker_with_concurrency(pool, 1)
    }

    fn spawn_worker_with_concurrency(
        pool: PgPool,
        concurrency: usize,
    ) -> tokio::task::JoinHandle<crate::Result<()>> {
        tokio::spawn(async move {
            Worker::<TestTask>::new(pool)
                .with_concurrency(concurrency)
                .run()
                .await
        })
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

    #[tokio::test]
    async fn finish_run_waits_for_inflight_steps_before_returning_errors() {
        let worker = Arc::new(
            Worker::<TestTask>::new(
                PgPoolOptions::new()
                    .connect_lazy("postgres:///pg_task")
                    .unwrap(),
            )
            .with_concurrency(1),
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
    async fn run_unlocks_stale_tasks_before_processing(pool: PgPool) {
        let state = StepStateGuard::new();
        insert_task(
            &pool,
            &TestTask::Complete(Complete { key: state.key() }),
            true,
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

        assert_eq!(state.state().events(), vec!["complete"]);
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
