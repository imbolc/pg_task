use crate::{
    task::Task,
    util::{chrono_duration_to_std, db_error, wait_for_reconnection},
    waiter::Waiter,
    Error, Result, Step, LOST_CONNECTION_SLEEP,
};
use chrono::Utc;
use sqlx::postgres::PgPool;
use std::{marker::PhantomData, sync::Arc};
use tokio::{sync::Semaphore, time::sleep};
use tracing::{debug, error, trace, warn};

/// A worker for processing tasks
pub struct Worker<T> {
    db: PgPool,
    waiter: Waiter,
    tasks: PhantomData<T>,
    concurrency: usize,
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
    pub async fn run(&self) -> Result<()> {
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
                        .map_err(Error::UnreachableWorkerSemaphoreClosed)?;
                    let db = self.db.clone();
                    tokio::spawn(async move {
                        if let Err(e) = task.run_step::<S>(&db).await {
                            error!("[{}] {}", task.id, source_chain::to_string(&e));
                        };
                        drop(permit);
                    });
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

    /// Waits until the next task is ready, marks it as running and returns it.
    async fn recv_task(&self) -> Result<Task> {
        trace!("Receiving the next task");

        loop {
            let table_changes = self.waiter.subscribe();
            let mut tx = self.db.begin().await.map_err(db_error!("begin"))?;
            if let Some(task) = Task::fetch_closest(&mut tx).await? {
                let time_to_run = task.wakeup_at - Utc::now();
                if time_to_run <= chrono::Duration::zero() {
                    task.mark_running(&mut tx).await?;
                    tx.commit()
                        .await
                        .map_err(db_error!("commit on task return"))?;
                    return Ok(task);
                }
                tx.commit()
                    .await
                    .map_err(db_error!("commit on wait for a period"))?;
                table_changes
                    .wait_for(chrono_duration_to_std(time_to_run))
                    .await;
            } else {
                tx.commit()
                    .await
                    .map_err(db_error!("commit on wait forever"))?;
                table_changes.wait_forever().await;
            }
        }
    }
}
