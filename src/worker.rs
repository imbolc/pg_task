use crate::{
    listener::Listener,
    task::{RenewedTaskLease, Task, WorkerLease},
    util::{
        db_error, db_interruption, std_duration_to_chrono, wait_for_reconnection, DbInterruption,
    },
    Error, Result, Step, LOST_CONNECTION_SLEEP,
};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use sqlx::postgres::PgPool;
use std::{marker::PhantomData, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, Semaphore},
    time::{interval, sleep, MissedTickBehavior},
};
use tracing::{error, info, trace, warn};
use uuid::Uuid;

const LOCKED_TASK_RECHECK_DELAY: Duration = Duration::from_millis(100);
const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

enum HeartbeatEvent {
    Failed,
    Recovered,
    Expired(Error),
}

struct RunEvents {
    heartbeat: mpsc::UnboundedReceiver<HeartbeatEvent>,
    step_errors: mpsc::UnboundedReceiver<Error>,
}

struct RunningStep {
    task_id: Uuid,
    abort_handle: tokio::task::AbortHandle,
    lock_expires_at: DateTime<Utc>,
}

enum TaskAvailability {
    Ready,
    Stopped,
}

/// A worker for processing tasks
pub struct Worker<T> {
    db: PgPool,
    listener: Listener,
    tasks: PhantomData<T>,
    concurrency: NonZeroUsize,
    lease_timeout: Duration,
    heartbeat_interval: Duration,
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
            lease_timeout: DEFAULT_LEASE_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            tasks: PhantomData,
        }
    }

    /// Sets the number of concurrent tasks, default is the number of CPU cores
    pub fn with_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Sets the task lease timeout.
    ///
    /// If a worker cannot renew this lease before it expires, another worker
    /// may reclaim the task.
    pub fn with_lease_timeout(mut self, lease_timeout: Duration) -> Self {
        assert!(!lease_timeout.is_zero(), "lease timeout must be non-zero");
        self.lease_timeout = lease_timeout;
        self
    }

    /// Sets how often the worker renews leases for its running tasks.
    pub fn with_heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        assert!(
            !heartbeat_interval.is_zero(),
            "heartbeat interval must be non-zero"
        );
        self.heartbeat_interval = heartbeat_interval;
        self
    }

    /// Runs all ready tasks to completion and waits for new ones
    pub async fn run(&self) -> Result<()> {
        self.validate_lease_timing();
        self.listener.listen(self.db.clone()).await?;

        let lease = WorkerLease::new(Uuid::new_v4(), self.lease_timeout);
        let semaphore = Arc::new(Semaphore::new(self.concurrency.get()));
        let running_steps = Arc::new(Mutex::new(Vec::new()));
        let (heartbeat_events_sender, mut heartbeat_events) = mpsc::unbounded_channel();
        let heartbeat = self.spawn_heartbeat(heartbeat_events_sender, running_steps.clone(), lease);
        let (step_error_sender, mut step_errors) = mpsc::unbounded_channel();
        let mut heartbeat_healthy = true;
        let mut abort_running_steps = false;
        let mut reserved_permit = None;

        let result = loop {
            let availability = tokio::select! {
                biased;

                Some(error) = step_errors.recv() => {
                    drop(reserved_permit.take());
                    break Err(error);
                }
                Some(event) = heartbeat_events.recv() => {
                    if let Err(error) = Self::handle_heartbeat_event(event, &mut heartbeat_healthy) {
                        abort_running_steps = true;
                        drop(reserved_permit.take());
                        break Err(error);
                    }
                    if !heartbeat_healthy {
                        drop(reserved_permit.take());
                    }
                    continue;
                }
                _ = sleep(LOCKED_TASK_RECHECK_DELAY), if !heartbeat_healthy => {
                    if self.listener.time_to_stop_worker() {
                        drop(reserved_permit.take());
                        break Ok(());
                    }
                    if let Some(error) = self.listener.take_error() {
                        if let Err(error) = self
                            .handle_recv_task_error_or_heartbeat(
                                error,
                                &mut heartbeat_events,
                                &mut heartbeat_healthy,
                                &mut abort_running_steps,
                            )
                            .await
                        {
                            drop(reserved_permit.take());
                            break Err(error);
                        }
                    }
                    continue;
                }
                permit = semaphore.clone().acquire_owned(), if heartbeat_healthy && reserved_permit.is_none() => {
                    reserved_permit = Some(permit.map_err(Error::UnreachableWorkerSemaphoreClosed)?);
                    continue;
                }
                availability = self.wait_for_available_task(), if heartbeat_healthy && reserved_permit.is_some() => availability,
            };
            match availability {
                Ok(TaskAvailability::Ready) => match self.claim_available_task(lease).await {
                    Ok(Some((task, step, lease, lock_expires_at))) => {
                        let permit = reserved_permit
                            .take()
                            .expect("task claiming requires a reserved semaphore permit");
                        let db = self.db.clone();
                        let step_error_sender = step_error_sender.clone();
                        let task_id = task.id;
                        let step = tokio::spawn(async move {
                            if let Err(e) = task.run_step(&db, step, lease).await {
                                error!("[{}] {}", task.id, source_chain::to_string(&e));
                                let _ = step_error_sender.send(e);
                            };
                            drop(permit);
                        });
                        Self::track_running_step(
                            &running_steps,
                            task_id,
                            step.abort_handle(),
                            lock_expires_at,
                        );
                    }
                    Ok(None) => continue,
                    Err(e) => {
                        drop(reserved_permit.take());
                        if let Err(error) = self
                            .handle_recv_task_error_or_heartbeat(
                                e,
                                &mut heartbeat_events,
                                &mut heartbeat_healthy,
                                &mut abort_running_steps,
                            )
                            .await
                        {
                            drop(reserved_permit.take());
                            break Err(error);
                        }
                    }
                },
                Ok(TaskAvailability::Stopped) => {
                    drop(reserved_permit.take());
                    break Ok(());
                }
                Err(e) => {
                    drop(reserved_permit.take());
                    if let Err(error) = self
                        .handle_recv_task_error_or_heartbeat(
                            e,
                            &mut heartbeat_events,
                            &mut heartbeat_healthy,
                            &mut abort_running_steps,
                        )
                        .await
                    {
                        drop(reserved_permit.take());
                        break Err(error);
                    }
                }
            }
        };
        self.finish_run(
            result,
            semaphore,
            heartbeat,
            running_steps,
            abort_running_steps,
            RunEvents {
                heartbeat: heartbeat_events,
                step_errors,
            },
        )
        .await
    }

    fn validate_lease_timing(&self) {
        assert!(
            self.heartbeat_interval < self.lease_timeout,
            "heartbeat interval must be shorter than lease timeout"
        );
    }

    /// Waits until a task may be claimed without mutating task leases.
    async fn wait_for_available_task(&self) -> Result<TaskAvailability> {
        trace!("Waiting for an available task");
        loop {
            if self.listener.time_to_stop_worker() {
                return Ok(TaskAvailability::Stopped);
            }

            if let Some(error) = self.listener.take_error() {
                return Err(error);
            }

            let table_changes = self.listener.subscribe();
            let mut tx = self.db.begin().await.map_err(db_error!("begin"))?;

            if Task::fetch_ready(&mut tx).await?.is_some() {
                tx.commit().await.map_err(db_error!("ready task check"))?;
                return Ok(TaskAvailability::Ready);
            }

            let next_available_at = Task::fetch_next_available_at(&mut tx).await?;
            tx.commit().await.map_err(db_error!("no ready tasks"))?;

            if let Some(available_at) = next_available_at {
                let delay = Task::delay_until(available_at).unwrap_or(LOCKED_TASK_RECHECK_DELAY);
                table_changes.wait_for(delay).await;
            } else {
                table_changes.wait_forever().await;
            }
        }
    }

    /// Claims a currently available task lease.
    async fn claim_available_task(
        &self,
        lease: WorkerLease,
    ) -> Result<Option<(Task, S, WorkerLease, DateTime<Utc>)>> {
        trace!("Claiming an available task");
        let mut tx = self.db.begin().await.map_err(db_error!("begin"))?;

        let Some(task) = Task::fetch_ready(&mut tx).await? else {
            tx.commit().await.map_err(db_error!("no ready tasks"))?;
            return Ok(None);
        };

        let Some(claimed) = task.claim(&mut tx, lease).await? else {
            tx.commit().await.map_err(db_error!("save error"))?;
            return Ok(None);
        };
        tx.commit().await.map_err(db_error!("claim lease"))?;
        Ok(Some((task, claimed.step, lease, claimed.lock_expires_at)))
    }

    /// Waits until the next task is ready, claims its lease and returns it.
    /// Returns `None` if the worker is stopped
    #[cfg(test)]
    async fn recv_task(
        &self,
        lease: WorkerLease,
    ) -> Result<Option<(Task, S, WorkerLease, DateTime<Utc>)>> {
        trace!("Receiving the next task");

        loop {
            match self.wait_for_available_task().await? {
                TaskAvailability::Ready => {}
                TaskAvailability::Stopped => return Ok(None),
            }

            if let Some(task) = self.claim_available_task(lease).await? {
                return Ok(Some(task));
            }
        }
    }

    fn handle_heartbeat_event(event: HeartbeatEvent, heartbeat_healthy: &mut bool) -> Result<()> {
        match event {
            HeartbeatEvent::Failed => {
                if *heartbeat_healthy {
                    warn!("Task fetching paused because task leases are not renewing");
                }
                *heartbeat_healthy = false;
                Ok(())
            }
            HeartbeatEvent::Recovered => {
                if !*heartbeat_healthy {
                    warn!("Task lease renewal recovered; task fetching resumed");
                }
                *heartbeat_healthy = true;
                Ok(())
            }
            HeartbeatEvent::Expired(error) => Err(error),
        }
    }

    async fn handle_recv_task_error_or_heartbeat(
        &self,
        error: Error,
        heartbeat_events: &mut mpsc::UnboundedReceiver<HeartbeatEvent>,
        heartbeat_healthy: &mut bool,
        abort_running_steps: &mut bool,
    ) -> Result<()> {
        let handle_error = self.handle_recv_task_error(error);
        tokio::pin!(handle_error);

        loop {
            tokio::select! {
                result = &mut handle_error => return result,
                Some(event) = heartbeat_events.recv() => {
                    if let Err(error) = Self::handle_heartbeat_event(event, heartbeat_healthy) {
                        *abort_running_steps = true;
                        return Err(error);
                    }
                }
            }
        }
    }

    async fn handle_recv_task_error(&self, error: Error) -> Result<()> {
        let interruption = match &error {
            Error::Db(db_error, _) => db_interruption(db_error),
            _ => DbInterruption::Permanent,
        };

        match interruption {
            DbInterruption::Connection => {
                warn!(
                    "Task fetching stopped because the database connection was interrupted:\n{}",
                    source_chain::to_string(&error)
                );
                sleep(LOST_CONNECTION_SLEEP).await;
                wait_for_reconnection(&self.db, LOST_CONNECTION_SLEEP).await?;
                warn!("Task fetching resumed");
                Ok(())
            }
            DbInterruption::PoolTimeout => {
                warn!(
                    "Task fetching is waiting for a free database connection from the pool:\n{}",
                    source_chain::to_string(&error)
                );
                sleep(LOST_CONNECTION_SLEEP).await;
                Ok(())
            }
            DbInterruption::Permanent => Err(error),
        }
    }

    fn spawn_heartbeat(
        &self,
        events: mpsc::UnboundedSender<HeartbeatEvent>,
        running_steps: Arc<Mutex<Vec<RunningStep>>>,
        lease: WorkerLease,
    ) -> tokio::task::AbortHandle {
        self.validate_lease_timing();
        let db = self.db.clone();
        let mut heartbeat = interval(self.heartbeat_interval);
        let heartbeat_interval = self.heartbeat_interval;
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tokio::spawn(async move {
            let mut renewal_failed = false;
            heartbeat.tick().await;
            loop {
                heartbeat.tick().await;
                let running_task_ids = Self::running_task_ids(&running_steps);
                if running_task_ids.is_empty() {
                    if renewal_failed {
                        let _ = events.send(HeartbeatEvent::Recovered);
                        renewal_failed = false;
                    }
                    continue;
                }
                match Task::renew_leases(&db, lease, &running_task_ids).await {
                    Ok(renewed_leases)
                        if Self::update_running_lease_expirations(
                            &running_task_ids,
                            &renewed_leases,
                            &running_steps,
                        ) =>
                    {
                        trace!("Renewed {} task leases", renewed_leases.len());
                        if renewal_failed {
                            let _ = events.send(HeartbeatEvent::Recovered);
                            renewal_failed = false;
                        }
                    }
                    Ok(renewed_leases) => {
                        warn!(
                            "Task lease renewal updated {} of {} running task leases",
                            renewed_leases.len(),
                            running_task_ids.len()
                        );
                        if !renewal_failed {
                            let _ = events.send(HeartbeatEvent::Failed);
                            renewal_failed = true;
                        }
                        if Self::running_lease_expires_before_next_heartbeat(
                            &running_steps,
                            heartbeat_interval,
                        ) {
                            let _ = events.send(HeartbeatEvent::Expired(Error::TaskLeaseExpired));
                            break;
                        }
                    }
                    Err(error) => {
                        warn!(
                            "Task lease renewal failed:\n{}",
                            source_chain::to_string(&error)
                        );
                        if !renewal_failed {
                            let _ = events.send(HeartbeatEvent::Failed);
                            renewal_failed = true;
                        }
                        if Self::running_lease_expires_before_next_heartbeat(
                            &running_steps,
                            heartbeat_interval,
                        ) {
                            let _ = events.send(HeartbeatEvent::Expired(error));
                            break;
                        }
                    }
                }
            }
        })
        .abort_handle()
    }

    fn track_running_step(
        running_steps: &Mutex<Vec<RunningStep>>,
        task_id: Uuid,
        abort_handle: tokio::task::AbortHandle,
        lock_expires_at: DateTime<Utc>,
    ) {
        let mut running_steps = running_steps.lock();
        running_steps.retain(|step| !step.abort_handle.is_finished());
        running_steps.push(RunningStep {
            task_id,
            abort_handle,
            lock_expires_at,
        });
    }

    fn abort_running_steps(running_steps: &Mutex<Vec<RunningStep>>) {
        let running_steps = running_steps.lock();
        for step in &*running_steps {
            step.abort_handle.abort();
        }
    }

    #[cfg(test)]
    fn has_running_steps(running_steps: &Mutex<Vec<RunningStep>>) -> bool {
        !Self::running_task_ids(running_steps).is_empty()
    }

    fn running_task_ids(running_steps: &Mutex<Vec<RunningStep>>) -> Vec<Uuid> {
        let mut running_steps = running_steps.lock();
        running_steps.retain(|step| !step.abort_handle.is_finished());
        running_steps.iter().map(|step| step.task_id).collect()
    }

    fn update_running_lease_expirations(
        running_task_ids: &[Uuid],
        renewed_leases: &[RenewedTaskLease],
        running_steps: &Mutex<Vec<RunningStep>>,
    ) -> bool {
        let mut running_steps = running_steps.lock();
        running_steps.retain(|step| !step.abort_handle.is_finished());
        let mut all_running_leases_renewed = true;

        for step in running_steps.iter_mut() {
            if !running_task_ids.contains(&step.task_id) {
                continue;
            }

            if let Some(renewed_lease) = renewed_leases
                .iter()
                .find(|renewed_lease| renewed_lease.task_id == step.task_id)
            {
                step.lock_expires_at = renewed_lease.lock_expires_at;
            } else {
                all_running_leases_renewed = false;
            }
        }

        all_running_leases_renewed
    }

    fn running_lease_expires_before_next_heartbeat(
        running_steps: &Mutex<Vec<RunningStep>>,
        heartbeat_interval: Duration,
    ) -> bool {
        let next_heartbeat_at = Utc::now() + std_duration_to_chrono(heartbeat_interval);
        let mut running_steps = running_steps.lock();
        running_steps.retain(|step| !step.abort_handle.is_finished());
        running_steps
            .iter()
            .any(|step| step.lock_expires_at <= next_heartbeat_at)
    }

    async fn finish_run(
        &self,
        result: Result<()>,
        semaphore: Arc<Semaphore>,
        heartbeat: tokio::task::AbortHandle,
        running_steps: Arc<Mutex<Vec<RunningStep>>>,
        abort_running_steps: bool,
        events: RunEvents,
    ) -> Result<()> {
        self.listener.shutdown();
        if abort_running_steps {
            heartbeat.abort();
            Self::abort_running_steps(&running_steps);
        }
        // Drain in-flight steps before returning so a restarted worker can't
        // reclaim them as stale while they are still running.
        let result = if abort_running_steps {
            self.wait_for_steps_to_finish(semaphore).await;
            result
        } else {
            match self
                .wait_for_steps_to_finish_or_events(
                    semaphore.clone(),
                    events.heartbeat,
                    events.step_errors,
                    result,
                )
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    Self::abort_running_steps(&running_steps);
                    self.wait_for_steps_to_finish(semaphore).await;
                    Err(error)
                }
            }
        };
        heartbeat.abort();
        if result.is_ok() {
            info!("Stopped");
        }
        result
    }

    async fn wait_for_steps_to_finish(&self, semaphore: Arc<Semaphore>) {
        self.wait_for_steps_to_finish_impl(semaphore, None, None, Ok(()))
            .await
            .expect("waiting without event receivers cannot fail")
            .expect("waiting without step errors cannot fail");
    }

    async fn wait_for_steps_to_finish_impl(
        &self,
        semaphore: Arc<Semaphore>,
        mut heartbeat_events: Option<mpsc::UnboundedReceiver<HeartbeatEvent>>,
        mut step_errors: Option<mpsc::UnboundedReceiver<Error>>,
        mut result: Result<()>,
    ) -> Result<Result<()>> {
        let mut logged_tasks_left = None;
        let mut heartbeat_healthy = true;
        loop {
            let tasks_left = self.concurrency.get() - semaphore.available_permits();
            if tasks_left == 0 {
                if let Some(step_errors) = step_errors.as_mut() {
                    Self::record_step_errors(step_errors, &mut result);
                }
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
            match (heartbeat_events.as_mut(), step_errors.as_mut()) {
                (Some(heartbeat_events), Some(step_errors)) => {
                    tokio::select! {
                        Some(event) = heartbeat_events.recv() => {
                            Self::handle_heartbeat_event(event, &mut heartbeat_healthy)?;
                        }
                        Some(error) = step_errors.recv() => {
                            Self::record_step_error(error, &mut result);
                        }
                        _ = sleep(Duration::from_secs_f32(0.1)) => {}
                    }
                }
                (Some(heartbeat_events), None) => {
                    tokio::select! {
                        Some(event) = heartbeat_events.recv() => {
                            Self::handle_heartbeat_event(event, &mut heartbeat_healthy)?;
                        }
                        _ = sleep(Duration::from_secs_f32(0.1)) => {}
                    }
                }
                (None, Some(step_errors)) => {
                    tokio::select! {
                        Some(error) = step_errors.recv() => {
                            Self::record_step_error(error, &mut result);
                        }
                        _ = sleep(Duration::from_secs_f32(0.1)) => {}
                    }
                }
                (None, None) => {
                    sleep(Duration::from_secs_f32(0.1)).await;
                }
            }
        }
        if logged_tasks_left.is_some() {
            trace!("The current step of every task is done")
        }
        Ok(result)
    }

    fn record_step_errors(
        step_errors: &mut mpsc::UnboundedReceiver<Error>,
        result: &mut Result<()>,
    ) {
        while let Ok(error) = step_errors.try_recv() {
            Self::record_step_error(error, result);
        }
    }

    fn record_step_error(error: Error, result: &mut Result<()>) {
        if result.is_ok() {
            *result = Err(error);
        }
    }

    async fn wait_for_steps_to_finish_or_events(
        &self,
        semaphore: Arc<Semaphore>,
        heartbeat_events: mpsc::UnboundedReceiver<HeartbeatEvent>,
        step_errors: mpsc::UnboundedReceiver<Error>,
        result: Result<()>,
    ) -> Result<Result<()>> {
        self.wait_for_steps_to_finish_impl(
            semaphore,
            Some(heartbeat_events),
            Some(step_errors),
            result,
        )
        .await
    }
}

#[cfg(test)]
mod tests;
