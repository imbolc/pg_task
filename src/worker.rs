use crate::{
    listener::Listener,
    task::{Task, TaskLease},
    util::{db_error, is_connection_error, is_pool_timeout, wait_for_reconnection},
    Error, Result, Step, LOST_CONNECTION_SLEEP,
};
use sqlx::postgres::PgPool;
use std::{
    marker::PhantomData,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
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

        let lease = TaskLease::new(Uuid::new_v4(), self.lease_timeout);
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
                    Ok(Some((task, step, lease))) => {
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
                        Self::track_running_step(&running_steps, task_id, step.abort_handle());
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

    /// Claims a currently available task and marks it running.
    async fn claim_available_task(&self, lease: TaskLease) -> Result<Option<(Task, S, TaskLease)>> {
        trace!("Claiming an available task");
        let mut tx = self.db.begin().await.map_err(db_error!("begin"))?;

        let Some(task) = Task::fetch_ready(&mut tx).await? else {
            tx.commit().await.map_err(db_error!("no ready tasks"))?;
            return Ok(None);
        };

        let Some(step) = task.claim(&mut tx, lease).await? else {
            tx.commit().await.map_err(db_error!("save error"))?;
            return Ok(None);
        };
        tx.commit().await.map_err(db_error!("mark running"))?;
        Ok(Some((task, step, lease)))
    }

    /// Waits until the next task is ready, marks it running and returns it.
    /// Returns `None` if the worker is stopped
    #[cfg(test)]
    async fn recv_task(&self, lease: TaskLease) -> Result<Option<(Task, S, TaskLease)>> {
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

    fn spawn_heartbeat(
        &self,
        events: mpsc::UnboundedSender<HeartbeatEvent>,
        running_steps: Arc<Mutex<Vec<RunningStep>>>,
        lease: TaskLease,
    ) -> tokio::task::AbortHandle {
        self.validate_lease_timing();
        let db = self.db.clone();
        let mut heartbeat = interval(self.heartbeat_interval);
        let heartbeat_interval = self.heartbeat_interval;
        let lease_timeout = self.lease_timeout;
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tokio::spawn(async move {
            let mut last_renewed_at = Instant::now();
            let mut renewal_failed = false;
            heartbeat.tick().await;
            loop {
                heartbeat.tick().await;
                let running_task_ids = Self::running_task_ids(&running_steps);
                if running_task_ids.is_empty() {
                    last_renewed_at = Instant::now();
                    if renewal_failed {
                        let _ = events.send(HeartbeatEvent::Recovered);
                        renewal_failed = false;
                    }
                    continue;
                }
                match Task::renew_leases(&db, lease, &running_task_ids).await {
                    Ok(renewed_task_ids)
                        if Self::renewed_all_running_leases(
                            &running_task_ids,
                            &renewed_task_ids,
                            &running_steps,
                        ) =>
                    {
                        trace!("Renewed {} task leases", renewed_task_ids.len());
                        last_renewed_at = Instant::now();
                        if renewal_failed {
                            let _ = events.send(HeartbeatEvent::Recovered);
                            renewal_failed = false;
                        }
                    }
                    Ok(renewed_task_ids) => {
                        warn!(
                            "Task lease renewal updated {} of {} running task leases",
                            renewed_task_ids.len(),
                            running_task_ids.len()
                        );
                        if !renewal_failed {
                            let _ = events.send(HeartbeatEvent::Failed);
                            renewal_failed = true;
                        }
                        if last_renewed_at.elapsed().saturating_add(heartbeat_interval)
                            >= lease_timeout
                        {
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
                        if !Self::running_task_ids(&running_steps).is_empty()
                            && last_renewed_at.elapsed().saturating_add(heartbeat_interval)
                                >= lease_timeout
                        {
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
    ) {
        let mut running_steps = running_steps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        running_steps.retain(|step| !step.abort_handle.is_finished());
        running_steps.push(RunningStep {
            task_id,
            abort_handle,
        });
    }

    fn abort_running_steps(running_steps: &Mutex<Vec<RunningStep>>) {
        let running_steps = running_steps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for step in &*running_steps {
            step.abort_handle.abort();
        }
    }

    #[cfg(test)]
    fn has_running_steps(running_steps: &Mutex<Vec<RunningStep>>) -> bool {
        !Self::running_task_ids(running_steps).is_empty()
    }

    fn running_task_ids(running_steps: &Mutex<Vec<RunningStep>>) -> Vec<Uuid> {
        let mut running_steps = running_steps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        running_steps.retain(|step| !step.abort_handle.is_finished());
        running_steps.iter().map(|step| step.task_id).collect()
    }

    fn renewed_all_running_leases(
        running_task_ids: &[Uuid],
        renewed_task_ids: &[Uuid],
        running_steps: &Mutex<Vec<RunningStep>>,
    ) -> bool {
        let still_running_task_ids = Self::running_task_ids(running_steps);
        running_task_ids.iter().all(|task_id| {
            renewed_task_ids.contains(task_id) || !still_running_task_ids.contains(task_id)
        })
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
mod tests {
    use super::{
        HeartbeatEvent, RunEvents, RunningStep, TaskAvailability, Worker,
        DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_LEASE_TIMEOUT,
    };
    use crate::{task::TaskLease, Error, NextStep, Step};
    use chrono::{Duration as ChronoDuration, Utc};
    use sqlx::{
        postgres::{PgConnectOptions, PgPoolOptions},
        PgPool,
    };
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
        sync::{mpsc, Notify, Semaphore},
        time::{sleep, timeout},
    };
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

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct FailStep {
        key: u64,
    }

    crate::task!(TestTask {
        Noop,
        Advance,
        Finish,
        Complete,
        Blocking,
        FailSavingError,
        FailStep,
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

    #[async_trait::async_trait]
    impl Step<TestTask> for FailStep {
        async fn step(self, _db: &PgPool) -> crate::StepResult<TestTask> {
            step_state(self.key).record("started");
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
        is_leased: bool,
        error: Option<&str>,
    ) -> Uuid {
        let (locked_by, lock_expires_at) = if is_leased {
            (
                Some(Uuid::from_u128(1)),
                Some(Utc::now() + ChronoDuration::seconds(60)),
            )
        } else {
            (None, None)
        };
        sqlx::query!(
            "
            INSERT INTO pg_task (step, wakeup_at, locked_by, lock_expires_at, error)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            ",
            step,
            wakeup_at,
            locked_by,
            lock_expires_at,
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
        is_leased: bool,
    ) -> Uuid {
        insert_raw_task(
            pool,
            &serde_json::to_string(step).unwrap(),
            wakeup_at,
            is_leased,
            None,
        )
        .await
    }

    async fn insert_task(pool: &PgPool, step: &TestTask, is_leased: bool) {
        insert_task_at(
            pool,
            step,
            Utc::now() - ChronoDuration::milliseconds(1),
            is_leased,
        )
        .await;
    }

    async fn set_task_lease(pool: &PgPool, id: Uuid, lock_expires_at: chrono::DateTime<Utc>) {
        set_task_lease_for_worker(pool, id, Uuid::from_u128(1), lock_expires_at).await;
    }

    async fn set_task_lease_for_worker(
        pool: &PgPool,
        id: Uuid,
        worker_id: Uuid,
        lock_expires_at: chrono::DateTime<Utc>,
    ) {
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

    async fn fetch_task_lease(pool: &PgPool, id: Uuid) -> Option<(Uuid, chrono::DateTime<Utc>)> {
        sqlx::query!(
            "
            SELECT locked_by, lock_expires_at
            FROM pg_task
            WHERE id = $1
            ",
            id,
        )
        .fetch_optional(pool)
        .await
        .unwrap()
        .map(|row| (row.locked_by.unwrap(), row.lock_expires_at.unwrap()))
    }

    fn idle_heartbeat() -> tokio::task::AbortHandle {
        tokio::spawn(async {
            std::future::pending::<()>().await;
        })
        .abort_handle()
    }

    fn idle_heartbeat_events() -> mpsc::UnboundedReceiver<HeartbeatEvent> {
        let (_sender, receiver) = mpsc::unbounded_channel();
        receiver
    }

    fn idle_step_errors() -> mpsc::UnboundedReceiver<Error> {
        let (_sender, receiver) = mpsc::unbounded_channel();
        receiver
    }

    fn idle_run_events() -> RunEvents {
        RunEvents {
            heartbeat: idle_heartbeat_events(),
            step_errors: idle_step_errors(),
        }
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
            .connect_with(current_database_options(&db_name))
            .await
            .unwrap()
    }

    // Connect to the database created by sqlx::test while keeping the
    // connection settings from DATABASE_URL. CI needs its TCP host and password;
    // postgres:///{db_name} only works for local peer-auth socket setups.
    fn current_database_options(db_name: &str) -> PgConnectOptions {
        std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set")
            .parse::<PgConnectOptions>()
            .unwrap()
            .database(db_name)
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

    fn worker_lease(worker: &Worker<TestTask>) -> TaskLease {
        TaskLease::new(Uuid::new_v4(), worker.lease_timeout)
    }

    fn running_step_entry(task_id: Uuid, abort_handle: tokio::task::AbortHandle) -> RunningStep {
        RunningStep {
            task_id,
            abort_handle,
        }
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

    #[tokio::test]
    #[should_panic(expected = "lease timeout must be non-zero")]
    async fn with_lease_timeout_rejects_zero() {
        Worker::<TestTask>::new(
            PgPoolOptions::new()
                .connect_lazy("postgres:///pg_task")
                .unwrap(),
        )
        .with_lease_timeout(Duration::ZERO);
    }

    #[tokio::test]
    #[should_panic(expected = "heartbeat interval must be non-zero")]
    async fn with_heartbeat_interval_rejects_zero() {
        Worker::<TestTask>::new(
            PgPoolOptions::new()
                .connect_lazy("postgres:///pg_task")
                .unwrap(),
        )
        .with_heartbeat_interval(Duration::ZERO);
    }

    #[tokio::test]
    #[should_panic(expected = "heartbeat interval must be shorter than lease timeout")]
    async fn run_rejects_lease_timeout_that_is_not_longer_than_the_heartbeat_interval() {
        let worker = Worker::<TestTask>::new(
            PgPoolOptions::new()
                .connect_lazy("postgres:///pg_task")
                .unwrap(),
        )
        .with_lease_timeout(DEFAULT_HEARTBEAT_INTERVAL);

        let _ = worker.run().await;
    }

    #[tokio::test]
    #[should_panic(expected = "heartbeat interval must be shorter than lease timeout")]
    async fn run_rejects_heartbeat_interval_that_is_not_shorter_than_the_lease_timeout() {
        let worker = Worker::<TestTask>::new(
            PgPoolOptions::new()
                .connect_lazy("postgres:///pg_task")
                .unwrap(),
        )
        .with_heartbeat_interval(DEFAULT_LEASE_TIMEOUT);

        let _ = worker.run().await;
    }

    #[test]
    fn heartbeat_events_pause_resume_and_expire_fetching() {
        let mut heartbeat_healthy = true;
        Worker::<TestTask>::handle_heartbeat_event(HeartbeatEvent::Failed, &mut heartbeat_healthy)
            .unwrap();
        assert!(!heartbeat_healthy);
        Worker::<TestTask>::handle_heartbeat_event(HeartbeatEvent::Failed, &mut heartbeat_healthy)
            .unwrap();
        assert!(!heartbeat_healthy);

        Worker::<TestTask>::handle_heartbeat_event(
            HeartbeatEvent::Recovered,
            &mut heartbeat_healthy,
        )
        .unwrap();
        assert!(heartbeat_healthy);
        Worker::<TestTask>::handle_heartbeat_event(
            HeartbeatEvent::Recovered,
            &mut heartbeat_healthy,
        )
        .unwrap();
        assert!(heartbeat_healthy);

        let err = Worker::<TestTask>::handle_heartbeat_event(
            HeartbeatEvent::Expired(Error::Db(sqlx::Error::PoolTimedOut, "test".into())),
            &mut heartbeat_healthy,
        )
        .unwrap_err();
        assert!(matches!(err, Error::Db(sqlx::Error::PoolTimedOut, _)));
    }

    #[tokio::test]
    async fn heartbeat_expiry_interrupts_retryable_fetch_error_handling() {
        init_tracing();
        let worker = Worker::<TestTask>::new(
            PgPoolOptions::new()
                .connect_lazy("postgres:///pg_task")
                .unwrap(),
        );
        let (heartbeat_events, mut heartbeat_events_receiver) = mpsc::unbounded_channel();
        heartbeat_events
            .send(HeartbeatEvent::Expired(Error::Db(
                sqlx::Error::PoolTimedOut,
                "heartbeat".into(),
            )))
            .unwrap();
        let mut heartbeat_healthy = true;
        let mut abort_running_steps = false;

        let err = timeout(
            Duration::from_millis(100),
            worker.handle_recv_task_error_or_heartbeat(
                Error::Db(sqlx::Error::PoolTimedOut, "fetch".into()),
                &mut heartbeat_events_receiver,
                &mut heartbeat_healthy,
                &mut abort_running_steps,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();

        assert!(matches!(err, Error::Db(sqlx::Error::PoolTimedOut, _)));
        assert!(abort_running_steps);
    }

    #[tokio::test]
    async fn heartbeat_recovery_preserves_retryable_fetch_error_handling() {
        init_tracing();
        let worker = Worker::<TestTask>::new(
            PgPoolOptions::new()
                .connect_lazy("postgres:///pg_task")
                .unwrap(),
        );
        let (heartbeat_events, mut heartbeat_events_receiver) = mpsc::unbounded_channel();
        heartbeat_events.send(HeartbeatEvent::Failed).unwrap();
        heartbeat_events.send(HeartbeatEvent::Recovered).unwrap();
        let mut heartbeat_healthy = true;
        let mut abort_running_steps = false;

        worker
            .handle_recv_task_error_or_heartbeat(
                Error::Db(sqlx::Error::PoolTimedOut, "fetch".into()),
                &mut heartbeat_events_receiver,
                &mut heartbeat_healthy,
                &mut abort_running_steps,
            )
            .await
            .unwrap();

        assert!(heartbeat_healthy);
        assert!(!abort_running_steps);
    }

    #[tokio::test]
    async fn heartbeat_failure_pauses_after_retryable_fetch_error_handling() {
        init_tracing();
        let worker = Worker::<TestTask>::new(
            PgPoolOptions::new()
                .connect_lazy("postgres:///pg_task")
                .unwrap(),
        );
        let (heartbeat_events, mut heartbeat_events_receiver) = mpsc::unbounded_channel();
        heartbeat_events.send(HeartbeatEvent::Failed).unwrap();
        let mut heartbeat_healthy = true;
        let mut abort_running_steps = false;

        worker
            .handle_recv_task_error_or_heartbeat(
                Error::Db(sqlx::Error::PoolTimedOut, "fetch".into()),
                &mut heartbeat_events_receiver,
                &mut heartbeat_healthy,
                &mut abort_running_steps,
            )
            .await
            .unwrap();

        assert!(!heartbeat_healthy);
        assert!(!abort_running_steps);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn heartbeat_skips_renewal_without_running_steps(pool: PgPool) {
        init_tracing();
        sqlx::query!("ALTER TABLE pg_task RENAME COLUMN lock_expires_at TO task_lock_expires_at")
            .execute(&pool)
            .await
            .unwrap();
        let worker = Worker::<TestTask>::new(pool)
            .with_lease_timeout(Duration::from_millis(80))
            .with_heartbeat_interval(Duration::from_millis(20));
        let (events, mut events_receiver) = mpsc::unbounded_channel();
        let heartbeat = worker.spawn_heartbeat(
            events,
            Arc::new(Mutex::new(Vec::new())),
            worker_lease(&worker),
        );

        assert!(timeout(Duration::from_millis(150), events_receiver.recv())
            .await
            .is_err());

        heartbeat.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn heartbeat_expires_when_running_steps_have_no_live_leases(pool: PgPool) {
        init_tracing();
        let worker = Worker::<TestTask>::new(pool)
            .with_lease_timeout(Duration::from_millis(80))
            .with_heartbeat_interval(Duration::from_millis(20));
        let running_step = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let running_steps = Arc::new(Mutex::new(vec![running_step_entry(
            Uuid::new_v4(),
            running_step.abort_handle(),
        )]));
        let (events, mut events_receiver) = mpsc::unbounded_channel();
        let heartbeat = worker.spawn_heartbeat(events, running_steps, worker_lease(&worker));

        let event = timeout(Duration::from_secs(1), events_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(event, HeartbeatEvent::Failed));

        let event = timeout(Duration::from_secs(1), events_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event,
            HeartbeatEvent::Expired(Error::TaskLeaseExpired)
        ));

        heartbeat.abort();
        running_step.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn heartbeat_expires_when_any_running_step_loses_its_lease(pool: PgPool) {
        init_tracing();
        let worker = Worker::<TestTask>::new(pool.clone())
            .with_lease_timeout(Duration::from_millis(80))
            .with_heartbeat_interval(Duration::from_millis(20));
        let worker_id = Uuid::new_v4();
        let lease = TaskLease::new(worker_id, worker.lease_timeout);
        let live = insert_task_at(
            &pool,
            &TestTask::Noop(Noop),
            Utc::now() - ChronoDuration::milliseconds(1),
            false,
        )
        .await;
        let expired = insert_task_at(
            &pool,
            &TestTask::Noop(Noop),
            Utc::now() - ChronoDuration::milliseconds(1),
            false,
        )
        .await;
        set_task_lease_for_worker(
            &pool,
            live,
            worker_id,
            Utc::now() + ChronoDuration::milliseconds(200),
        )
        .await;
        set_task_lease_for_worker(
            &pool,
            expired,
            worker_id,
            Utc::now() - ChronoDuration::milliseconds(1),
        )
        .await;
        let live_step = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let expired_step = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let running_steps = Arc::new(Mutex::new(vec![
            running_step_entry(live, live_step.abort_handle()),
            running_step_entry(expired, expired_step.abort_handle()),
        ]));
        let (events, mut events_receiver) = mpsc::unbounded_channel();
        let heartbeat = worker.spawn_heartbeat(events, running_steps, lease);

        let event = timeout(Duration::from_secs(1), events_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(event, HeartbeatEvent::Failed));

        let event = timeout(Duration::from_secs(1), events_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event,
            HeartbeatEvent::Expired(Error::TaskLeaseExpired)
        ));

        heartbeat.abort();
        live_step.abort();
        expired_step.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn heartbeat_skips_pool_timeouts_without_running_steps(pool: PgPool) {
        init_tracing();
        let worker_pool = connect_to_current_db(&pool, 1, Duration::from_millis(20)).await;
        let held_connection = worker_pool.acquire().await.unwrap();
        let worker = Worker::<TestTask>::new(worker_pool)
            .with_lease_timeout(Duration::from_millis(500))
            .with_heartbeat_interval(Duration::from_millis(20));
        let (events, mut events_receiver) = mpsc::unbounded_channel();
        let heartbeat = worker.spawn_heartbeat(
            events,
            Arc::new(Mutex::new(Vec::new())),
            worker_lease(&worker),
        );

        assert!(timeout(Duration::from_millis(150), events_receiver.recv())
            .await
            .is_err());

        drop(held_connection);
        heartbeat.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn heartbeat_reports_recovery_after_live_leases_are_renewed(pool: PgPool) {
        init_tracing();
        let worker_pool = connect_to_current_db(&pool, 1, Duration::from_millis(20)).await;
        let held_connection = worker_pool.acquire().await.unwrap();
        let worker = Worker::<TestTask>::new(worker_pool)
            .with_lease_timeout(Duration::from_millis(500))
            .with_heartbeat_interval(Duration::from_millis(20));
        let worker_id = Uuid::new_v4();
        let lease = TaskLease::new(worker_id, worker.lease_timeout);
        let id = insert_task_at(
            &pool,
            &TestTask::Noop(Noop),
            Utc::now() - ChronoDuration::milliseconds(1),
            false,
        )
        .await;
        let initial_expires_at = Utc::now() + ChronoDuration::milliseconds(200);
        sqlx::query!(
            "
            UPDATE pg_task
            SET locked_by = $2,
                lock_expires_at = $3
            WHERE id = $1
            ",
            id,
            worker_id,
            initial_expires_at,
        )
        .execute(&pool)
        .await
        .unwrap();
        let running_step = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let running_steps = Arc::new(Mutex::new(vec![running_step_entry(
            id,
            running_step.abort_handle(),
        )]));
        let (events, mut events_receiver) = mpsc::unbounded_channel();
        let heartbeat = worker.spawn_heartbeat(events, running_steps, lease);

        let event = timeout(Duration::from_secs(1), events_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(event, HeartbeatEvent::Failed));

        drop(held_connection);

        let event = timeout(Duration::from_secs(1), events_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(event, HeartbeatEvent::Recovered));

        let (_locked_by, renewed_expires_at) = fetch_task_lease(&pool, id).await.unwrap();
        assert!(renewed_expires_at > initial_expires_at);

        heartbeat.abort();
        running_step.abort();
    }

    #[tokio::test]
    async fn running_step_tracking_prunes_finished_steps() {
        let finished_step = tokio::spawn(async {});
        let finished_step_abort = finished_step.abort_handle();
        finished_step.await.unwrap();

        let running_step = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let running_step_abort = running_step.abort_handle();
        let running_steps = Mutex::new(vec![running_step_entry(
            Uuid::new_v4(),
            finished_step_abort,
        )]);

        Worker::<TestTask>::track_running_step(&running_steps, Uuid::new_v4(), running_step_abort);

        assert_eq!(
            running_steps
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1,
        );
        assert!(Worker::<TestTask>::has_running_steps(&running_steps));

        Worker::<TestTask>::abort_running_steps(&running_steps);
        assert!(running_step.await.unwrap_err().is_cancelled());
        assert!(!Worker::<TestTask>::has_running_steps(&running_steps));
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

        let lease = worker_lease(&worker);
        let err = worker.recv_task(lease).await.unwrap_err();

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

        let lease = worker_lease(&worker);
        assert!(worker.recv_task(lease).await.unwrap().is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn recv_task_returns_begin_errors_when_the_pool_is_closed(pool: PgPool) {
        let worker = Worker::<TestTask>::new(pool.clone());
        pool.close().await;

        let lease = worker_lease(&worker);
        let err = worker.recv_task(lease).await.unwrap_err();

        match err {
            Error::Db(sqlx::Error::PoolClosed, context) => {
                assert!(context.contains("begin"));
            }
            _ => panic!("expected a pool-closed begin error"),
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn wait_for_available_task_does_not_claim_ready_tasks(pool: PgPool) {
        let id = insert_task_at(
            &pool,
            &TestTask::Noop(Noop),
            Utc::now() - ChronoDuration::milliseconds(1),
            false,
        )
        .await;
        let worker = Worker::<TestTask>::new(pool.clone());

        let availability = worker.wait_for_available_task().await.unwrap();

        assert!(matches!(availability, TaskAvailability::Ready));
        let lease = sqlx::query!(
            "SELECT locked_by, lock_expires_at FROM pg_task WHERE id = $1",
            id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(lease.locked_by.is_none());
        assert!(lease.lock_expires_at.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn recv_task_stops_while_waiting_for_work(pool: PgPool) {
        let worker = Arc::new(Worker::<TestTask>::new(pool));
        let lease = worker_lease(&worker);
        let recv = tokio::spawn({
            let worker = worker.clone();
            async move { worker.recv_task(lease).await }
        });

        sleep(Duration::from_millis(50)).await;
        assert!(!recv.is_finished());
        worker.listener.stop_worker_for_tests();

        let received = timeout(Duration::from_secs(1), recv)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(received.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn recv_task_returns_listener_errors_while_waiting_for_work(pool: PgPool) {
        let worker = Arc::new(Worker::<TestTask>::new(pool));
        let lease = worker_lease(&worker);
        let recv = tokio::spawn({
            let worker = worker.clone();
            async move { worker.recv_task(lease).await }
        });

        sleep(Duration::from_millis(50)).await;
        assert!(!recv.is_finished());
        worker
            .listener
            .set_error_and_notify_for_tests(Error::ListenerReceive(sqlx::Error::Protocol(
                "listener failed".into(),
            )));

        let err = timeout(Duration::from_secs(1), recv)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            err,
            Error::ListenerReceive(sqlx::Error::Protocol(_))
        ));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn recv_task_skips_invalid_tasks_and_returns_next_ready_task(pool: PgPool) {
        let invalid_id = insert_raw_task(
            &pool,
            "not-json",
            Utc::now() - ChronoDuration::seconds(2),
            false,
            None,
        )
        .await;
        let expected = insert_task_at(
            &pool,
            &TestTask::Noop(Noop),
            Utc::now() - ChronoDuration::seconds(1),
            false,
        )
        .await;
        let worker = Worker::<TestTask>::new(pool.clone());
        let lease = worker_lease(&worker);

        let (task, step, _lease) = worker.recv_task(lease).await.unwrap().unwrap();

        assert_eq!(task.id, expected);
        assert!(matches!(step, TestTask::Noop(Noop)));
        let invalid_row = sqlx::query!(
            "SELECT locked_by, lock_expires_at, error FROM pg_task WHERE id = $1",
            invalid_id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(invalid_row.locked_by.is_none());
        assert!(invalid_row.lock_expires_at.is_none());
        assert!(invalid_row.error.is_some());
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
        let lease = worker_lease(&worker);
        let recv = tokio::spawn(async move { worker.recv_task(lease).await });

        sleep(Duration::from_millis(50)).await;
        assert!(!recv.is_finished());

        tx.rollback().await.unwrap();

        let (task, step, _lease) = timeout(Duration::from_secs(1), recv)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(task.id, id);
        assert!(matches!(step, TestTask::Noop(Noop)));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn recv_task_rechecks_leased_tasks_when_their_lease_expires(pool: PgPool) {
        let id = insert_task_at(
            &pool,
            &TestTask::Noop(Noop),
            Utc::now() - ChronoDuration::milliseconds(1),
            true,
        )
        .await;
        set_task_lease(&pool, id, Utc::now() + ChronoDuration::milliseconds(100)).await;

        let worker = Worker::<TestTask>::new(pool);
        let lease = worker_lease(&worker);
        let recv = tokio::spawn(async move { worker.recv_task(lease).await });

        sleep(Duration::from_millis(50)).await;
        assert!(!recv.is_finished());

        let (task, step, _lease) = timeout(Duration::from_secs(1), recv)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(task.id, id);
        assert!(matches!(step, TestTask::Noop(Noop)));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn recv_task_replaces_expired_lease_with_the_current_worker(pool: PgPool) {
        let id = insert_task_at(
            &pool,
            &TestTask::Noop(Noop),
            Utc::now() - ChronoDuration::milliseconds(1),
            true,
        )
        .await;
        set_task_lease(&pool, id, Utc::now() - ChronoDuration::milliseconds(1)).await;
        let worker = Worker::<TestTask>::new(pool.clone());
        let worker_id = Uuid::new_v4();
        let lease = TaskLease::new(worker_id, worker.lease_timeout);

        let (task, step, _lease) = worker.recv_task(lease).await.unwrap().unwrap();

        assert_eq!(task.id, id);
        assert!(matches!(step, TestTask::Noop(Noop)));
        let (locked_by, lock_expires_at) = fetch_task_lease(&pool, id).await.unwrap();
        assert_eq!(locked_by, worker_id);
        assert!(lock_expires_at > Utc::now());
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
        let first_lease = worker_lease(&first_worker);
        let second_lease = worker_lease(&second_worker);

        let first_recv = tokio::spawn(async move { first_worker.recv_task(first_lease).await });
        let second_recv = tokio::spawn(async move { second_worker.recv_task(second_lease).await });

        let (first_task, first_step, _first_lease) = timeout(Duration::from_secs(1), first_recv)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap();
        let (second_task, second_step, _second_lease) =
            timeout(Duration::from_secs(1), second_recv)
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

        let running = sqlx::query!("SELECT id FROM pg_task WHERE locked_by IS NOT NULL")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(running.len(), 2);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_renews_leases_for_running_tasks(pool: PgPool) {
        let state = StepStateGuard::new();
        let id = insert_task_at(
            &pool,
            &TestTask::Blocking(Blocking { key: state.key() }),
            Utc::now() - ChronoDuration::milliseconds(1),
            false,
        )
        .await;

        let worker = tokio::spawn({
            let pool = pool.clone();
            async move {
                Worker::<TestTask>::new(pool)
                    .with_concurrency(nonzero(1))
                    .with_lease_timeout(Duration::from_millis(200))
                    .with_heartbeat_interval(Duration::from_millis(50))
                    .run()
                    .await
            }
        });

        state.state().wait_for_events(1).await;
        let (locked_by, initial_expires_at) = fetch_task_lease(&pool, id).await.unwrap();

        sleep(Duration::from_millis(350)).await;

        let (renewed_by, renewed_expires_at) = fetch_task_lease(&pool, id).await.unwrap();
        assert_eq!(renewed_by, locked_by);
        assert!(renewed_expires_at > initial_expires_at);
        assert!(renewed_expires_at > Utc::now());

        stop_worker(&pool).await;
        state.state().release();

        timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_aborts_running_steps_when_heartbeat_cannot_renew_before_the_lease_expires(
        pool: PgPool,
    ) {
        let state = StepStateGuard::new();
        insert_task(
            &pool,
            &TestTask::Blocking(Blocking { key: state.key() }),
            false,
        )
        .await;

        let worker = tokio::spawn({
            let pool = pool.clone();
            async move {
                Worker::<TestTask>::new(pool)
                    .with_concurrency(nonzero(1))
                    .with_lease_timeout(Duration::from_millis(250))
                    .with_heartbeat_interval(Duration::from_millis(100))
                    .run()
                    .await
            }
        });

        state.state().wait_for_events(1).await;
        sleep(Duration::from_millis(50)).await;
        sqlx::query!("ALTER TABLE pg_task RENAME COLUMN lock_expires_at TO task_lock_expires_at")
            .execute(&pool)
            .await
            .unwrap();

        let err = timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        assert_eq!(state.state().events(), vec!["started"]);
        assert!(matches!(err, Error::Db(sqlx::Error::Database(_), _)));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_pauses_fetching_while_heartbeat_cannot_renew(pool: PgPool) {
        let worker_pool = connect_to_current_db(&pool, 1, Duration::from_millis(20)).await;
        let state = StepStateGuard::new();

        let worker = tokio::spawn(async move {
            Worker::<TestTask>::new(worker_pool)
                .with_concurrency(nonzero(1))
                .with_lease_timeout(Duration::from_millis(200))
                .with_heartbeat_interval(Duration::from_millis(50))
                .run()
                .await
        });

        sleep(Duration::from_millis(100)).await;
        insert_task(
            &pool,
            &TestTask::Complete(Complete { key: state.key() }),
            false,
        )
        .await;
        sleep(Duration::from_millis(150)).await;

        stop_worker(&pool).await;

        timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert!(state.state().events().is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_returns_listener_errors_while_fetching_is_paused(pool: PgPool) {
        let worker_pool = connect_to_current_db(&pool, 1, Duration::from_millis(20)).await;
        let worker = Arc::new(
            Worker::<TestTask>::new(worker_pool)
                .with_concurrency(nonzero(1))
                .with_lease_timeout(Duration::from_millis(200))
                .with_heartbeat_interval(Duration::from_millis(50)),
        );
        let run = tokio::spawn({
            let worker = worker.clone();
            async move { worker.run().await }
        });

        sleep(Duration::from_millis(1250)).await;
        worker
            .listener
            .set_error_for_tests(Error::ListenerReceive(sqlx::Error::Protocol(
                "listener failed".into(),
            )));

        let err = timeout(Duration::from_secs(3), run)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            err,
            Error::ListenerReceive(sqlx::Error::Protocol(_))
        ));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_keeps_waiting_after_retryable_errors_while_fetching_is_paused(pool: PgPool) {
        let worker_pool = connect_to_current_db(&pool, 1, Duration::from_millis(20)).await;
        let worker = Arc::new(
            Worker::<TestTask>::new(worker_pool)
                .with_concurrency(nonzero(1))
                .with_lease_timeout(Duration::from_millis(200))
                .with_heartbeat_interval(Duration::from_millis(50)),
        );
        let run = tokio::spawn({
            let worker = worker.clone();
            async move { worker.run().await }
        });

        sleep(Duration::from_millis(1250)).await;
        worker
            .listener
            .set_error_for_tests(Error::Db(sqlx::Error::PoolTimedOut, "fetch task".into()));
        sleep(Duration::from_millis(1300)).await;
        assert!(!run.is_finished());

        stop_worker(&pool).await;

        timeout(Duration::from_secs(3), run)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_resumes_fetching_after_heartbeat_recovers(pool: PgPool) {
        let worker_pool = connect_to_current_db(&pool, 2, Duration::from_millis(20)).await;
        let held_connection = worker_pool.acquire().await.unwrap();
        let state = StepStateGuard::new();

        let worker = tokio::spawn({
            let worker_pool = worker_pool.clone();
            async move {
                Worker::<TestTask>::new(worker_pool)
                    .with_concurrency(nonzero(1))
                    .with_lease_timeout(Duration::from_millis(300))
                    .with_heartbeat_interval(Duration::from_millis(50))
                    .run()
                    .await
            }
        });

        sleep(Duration::from_millis(100)).await;
        insert_task(
            &pool,
            &TestTask::Complete(Complete { key: state.key() }),
            false,
        )
        .await;
        sleep(Duration::from_millis(150)).await;

        assert!(state.state().events().is_empty());
        drop(held_connection);

        timeout(Duration::from_secs(3), async {
            loop {
                if !state.state().events().is_empty() {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        stop_worker(&pool).await;

        timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(state.state().events(), vec!["complete"]);
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
                        idle_heartbeat(),
                        Arc::new(Mutex::new(Vec::new())),
                        false,
                        idle_run_events(),
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
    async fn finish_run_returns_step_errors_received_while_draining() {
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
        let (step_error_sender, step_errors) = mpsc::unbounded_channel();

        let finish = tokio::spawn({
            let worker = worker.clone();
            let semaphore = semaphore.clone();
            async move {
                worker
                    .finish_run(
                        Ok(()),
                        semaphore,
                        idle_heartbeat(),
                        Arc::new(Mutex::new(Vec::new())),
                        false,
                        RunEvents {
                            heartbeat: idle_heartbeat_events(),
                            step_errors,
                        },
                    )
                    .await
            }
        });

        sleep(Duration::from_millis(50)).await;
        assert!(!finish.is_finished());

        step_error_sender
            .send(Error::Db(sqlx::Error::PoolTimedOut, "step".into()))
            .unwrap();
        drop(permit);

        let err = timeout(Duration::from_secs(1), finish)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, Error::Db(sqlx::Error::PoolTimedOut, _)));
    }

    #[tokio::test]
    async fn finish_run_keeps_heartbeat_alive_while_waiting_for_inflight_steps() {
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
        let heartbeat = tokio::spawn(async {
            std::future::pending::<()>().await;
        });

        let finish = tokio::spawn({
            let worker = worker.clone();
            let semaphore = semaphore.clone();
            let heartbeat_abort = heartbeat.abort_handle();
            async move {
                worker
                    .finish_run(
                        Ok(()),
                        semaphore,
                        heartbeat_abort,
                        Arc::new(Mutex::new(Vec::new())),
                        false,
                        idle_run_events(),
                    )
                    .await
            }
        });

        sleep(Duration::from_millis(50)).await;
        assert!(!finish.is_finished());
        assert!(!heartbeat.is_finished());

        drop(permit);

        timeout(Duration::from_secs(1), finish)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(heartbeat.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn finish_run_aborts_inflight_steps_when_lease_renewal_expires() {
        init_tracing();
        let worker = Worker::<TestTask>::new(
            PgPoolOptions::new()
                .connect_lazy("postgres:///pg_task")
                .unwrap(),
        )
        .with_concurrency(nonzero(1));
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let running_step = tokio::spawn(async move {
            let _permit = permit;
            std::future::pending::<()>().await;
        });
        let running_steps = Arc::new(Mutex::new(vec![running_step_entry(
            Uuid::new_v4(),
            running_step.abort_handle(),
        )]));

        let err = timeout(
            Duration::from_secs(1),
            worker.finish_run(
                Err(Error::Db(sqlx::Error::PoolTimedOut, "test".into())),
                semaphore,
                idle_heartbeat(),
                running_steps,
                true,
                idle_run_events(),
            ),
        )
        .await
        .unwrap()
        .unwrap_err();

        assert!(matches!(err, Error::Db(sqlx::Error::PoolTimedOut, _)));
        assert!(running_step.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn finish_run_aborts_inflight_steps_when_heartbeat_expires_while_draining() {
        init_tracing();
        let worker = Worker::<TestTask>::new(
            PgPoolOptions::new()
                .connect_lazy("postgres:///pg_task")
                .unwrap(),
        )
        .with_concurrency(nonzero(1));
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let running_step = tokio::spawn(async move {
            let _permit = permit;
            std::future::pending::<()>().await;
        });
        let running_steps = Arc::new(Mutex::new(vec![running_step_entry(
            Uuid::new_v4(),
            running_step.abort_handle(),
        )]));
        let (heartbeat_events_sender, heartbeat_events) = mpsc::unbounded_channel();
        heartbeat_events_sender
            .send(HeartbeatEvent::Expired(Error::TaskLeaseExpired))
            .unwrap();

        let err = timeout(
            Duration::from_secs(1),
            worker.finish_run(
                Ok(()),
                semaphore,
                idle_heartbeat(),
                running_steps,
                false,
                RunEvents {
                    heartbeat: heartbeat_events,
                    step_errors: idle_step_errors(),
                },
            ),
        )
        .await
        .unwrap()
        .unwrap_err();

        assert!(matches!(err, Error::TaskLeaseExpired));
        assert!(running_step.await.unwrap_err().is_cancelled());
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
    async fn run_stops_when_stop_notification_arrives_while_idle(pool: PgPool) {
        let worker = spawn_worker(pool.clone());

        sleep(Duration::from_millis(100)).await;
        stop_worker(&pool).await;

        timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
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
            "SELECT tried, locked_by, lock_expires_at, error FROM pg_task WHERE id = $1",
            invalid_id,
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(state.state().events(), vec!["complete"]);
        assert_eq!(invalid_row.tried, 0);
        assert!(invalid_row.locked_by.is_none());
        assert!(invalid_row.lock_expires_at.is_none());
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
    async fn run_returns_step_errors_received_after_stop_while_draining(pool: PgPool) {
        let state = StepStateGuard::new();
        insert_task(
            &pool,
            &TestTask::Blocking(Blocking { key: state.key() }),
            false,
        )
        .await;

        let worker = spawn_worker_with_concurrency(pool.clone(), 2);

        state.state().wait_for_events(1).await;
        stop_worker(&pool).await;
        sleep(Duration::from_millis(50)).await;
        assert!(!worker.is_finished());

        sqlx::query!("ALTER TABLE pg_task RENAME COLUMN id TO task_id")
            .execute(&pool)
            .await
            .unwrap();
        state.state().release();

        let err = timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        assert_eq!(state.state().events(), vec!["started", "completed"]);
        assert!(matches!(err, Error::Db(sqlx::Error::Database(_), _)));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_returns_spawned_step_persistence_errors(pool: PgPool) {
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
    async fn rerunning_worker_does_not_renew_abandoned_leases_from_previous_runs(pool: PgPool) {
        init_tracing();
        sqlx::query!("ALTER TABLE pg_task ADD CONSTRAINT reject_errors CHECK (error IS NULL)")
            .execute(&pool)
            .await
            .unwrap();
        let state = StepStateGuard::new();
        let id = insert_task_at(
            &pool,
            &TestTask::FailStep(FailStep { key: state.key() }),
            Utc::now() - ChronoDuration::milliseconds(1),
            false,
        )
        .await;
        let worker = Worker::<TestTask>::new(pool.clone())
            .with_concurrency(nonzero(1))
            .with_lease_timeout(Duration::from_secs(1))
            .with_heartbeat_interval(Duration::from_millis(50));

        let err = timeout(Duration::from_secs(1), worker.run())
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, Error::Db(sqlx::Error::Database(_), _)));
        let (abandoned_owner, abandoned_expires_at) = fetch_task_lease(&pool, id).await.unwrap();

        let rerun = tokio::spawn({
            let worker = worker;
            async move { worker.run().await }
        });

        sleep(Duration::from_millis(150)).await;
        let (locked_by, lock_expires_at) = fetch_task_lease(&pool, id).await.unwrap();
        assert_eq!(locked_by, abandoned_owner);
        assert_eq!(lock_expires_at, abandoned_expires_at);

        stop_worker(&pool).await;

        timeout(Duration::from_secs(1), rerun)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn run_returns_step_errors_from_spawned_tasks(pool: PgPool) {
        let state = StepStateGuard::new();
        sqlx::query!("ALTER TABLE pg_task ADD CONSTRAINT reject_errors CHECK (error IS NULL)")
            .execute(&pool)
            .await
            .unwrap();
        insert_task(
            &pool,
            &TestTask::FailStep(FailStep { key: state.key() }),
            false,
        )
        .await;

        let worker = spawn_worker(pool);

        state.state().wait_for_events(1).await;
        let err = timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        assert_eq!(state.state().events(), vec!["started"]);
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

    #[sqlx::test(migrations = "./migrations")]
    async fn run_respects_the_configured_concurrency_limit(pool: PgPool) {
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

        let worker = spawn_worker_with_concurrency(pool.clone(), 1);

        timeout(Duration::from_secs(1), async {
            loop {
                let started_count = usize::from(!first.state().events().is_empty())
                    + usize::from(!second.state().events().is_empty());
                if started_count == 1 {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        sleep(Duration::from_millis(100)).await;
        let first_started = !first.state().events().is_empty();
        let second_started = !second.state().events().is_empty();
        assert_ne!(first_started, second_started);

        if first_started {
            first.state().release();
            second.state().wait_for_events(1).await;
            stop_worker(&pool).await;
            second.state().release();
        } else {
            second.state().release();
            first.state().wait_for_events(1).await;
            stop_worker(&pool).await;
            first.state().release();
        }

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
