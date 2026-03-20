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
    use sqlx::{postgres::PgPoolOptions, PgPool};
    use std::{sync::Arc, time::Duration};
    use tokio::{sync::Semaphore, time::sleep};

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct NoopStep;

    crate::task!(TestTask { NoopStep });

    #[async_trait::async_trait]
    impl Step<TestTask> for NoopStep {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            Ok(NextStep::None)
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn handle_recv_task_error_returns_permanent_fetch_errors(pool: PgPool) {
        let err = sqlx::query("SELECT missing_column FROM pg_task")
            .execute(&pool)
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
}
