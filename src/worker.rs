use crate::{
    listener::Listener,
    task::Task,
    util::{db_error, wait_for_reconnection},
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

impl<S: Step<S>> Worker<S> {
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

        loop {
            match self.recv_task().await {
                Ok(Some(task)) => {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(Error::UnreachableWorkerSemaphoreClosed)?;
                    let db = self.db.clone();
                    tokio::spawn(async move {
                        if let Err(e) = task.run_step::<S>(&db).await {
                            error!("[{}] {}", task.id, source_chain::to_string(&e));
                        };
                        drop(permit);
                    });
                }
                Ok(None) => {
                    self.wait_for_steps_to_finish(semaphore.clone()).await;
                    info!("Stopped");
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "Can't fetch a task (probably due to db connection loss):\n{}",
                        source_chain::to_string(&e)
                    );
                    sleep(LOST_CONNECTION_SLEEP).await;
                    wait_for_reconnection(&self.db, LOST_CONNECTION_SLEEP).await;
                    warn!("Task fetching is probably restored");
                }
            }
        }
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
    async fn recv_task(&self) -> Result<Option<Task>> {
        trace!("Receiving the next task");

        loop {
            if self.listener.time_to_stop_worker() {
                return Ok(None);
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
                // Waiting until a task is ready or for the tasks table to change
                tx.commit().await.map_err(db_error!("wait"))?;
                table_changes.wait_for(delay).await;
                continue;
            };

            task.mark_running(&mut tx).await?;
            tx.commit().await.map_err(db_error!("mark running"))?;
            return Ok(Some(task));
        }
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
