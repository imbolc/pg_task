use crate::{util, LOST_CONNECTION_SLEEP};
use sqlx::{postgres::PgListener, PgPool};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::{futures::Notified, Notify},
    time::{sleep, timeout},
};
use tracing::{trace, warn};

const NOTIFICATION_CHANNEL: &str = "pg_task_changed";
const STOP_WORKER_NOTIFICATION: &str = "stop_worker";

/// Coordinates wakeups from task table changes, stop requests, and listener
/// failures
pub struct Listener {
    notify: Arc<Notify>,
    stop_worker: Arc<AtomicBool>,
    error: Arc<Mutex<Option<crate::Error>>>,
    task: Arc<Mutex<Option<tokio::task::AbortHandle>>>,
}

/// Subscription to the [`Listener`] notifications
pub struct Subscription<'a>(Notified<'a>);

impl Listener {
    /// Creates a listener
    pub fn new() -> Self {
        let notify = Arc::new(Notify::new());
        let stop_worker = Arc::new(AtomicBool::new(false));
        let error = Arc::new(Mutex::new(None));
        let task = Arc::new(Mutex::new(None));
        Self {
            notify,
            stop_worker,
            error,
            task,
        }
    }

    /// Connects to the db and starts to listen to tasks table changes
    pub async fn listen(&self, db: PgPool) -> crate::Result<()> {
        let mut listener = PgListener::connect_with(&db)
            .await
            .map_err(crate::Error::ListenerConnect)?;
        listener
            .listen(NOTIFICATION_CHANNEL)
            .await
            .map_err(crate::Error::ListenerListen)?;

        let notify = self.notify.clone();
        let stop_worker = self.stop_worker.clone();
        let error_slot = self.error.clone();
        let task = tokio::spawn(async move {
            loop {
                match listener.recv().await {
                    Ok(msg) => {
                        if msg.payload() == STOP_WORKER_NOTIFICATION {
                            trace!("Got stop-worker notification");
                            stop_worker.store(true, Ordering::SeqCst);
                            // Retain the wakeup if `recv_task()` has not subscribed yet.
                            notify.notify_one();
                            continue;
                        }
                    }
                    Err(e) => {
                        if Self::handle_recv_error(&error_slot, &notify, &db, e).await {
                            break;
                        }
                    }
                };
                notify.notify_waiters();
            }
        });
        Self::replace_task(&self.task, task.abort_handle());
        Ok(())
    }

    /// Subscribes for the next listener wakeup.
    ///
    /// Awaiting on the result ends on the first wakeup after the
    /// subscription, even if it happens between the subscription and awaiting.
    pub fn subscribe(&self) -> Subscription<'_> {
        Subscription(self.notify.notified())
    }

    /// Returns true if notification to stop worker is received
    pub fn time_to_stop_worker(&self) -> bool {
        self.stop_worker.load(Ordering::SeqCst)
    }

    pub(crate) fn take_error(&self) -> Option<crate::Error> {
        self.error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    pub(crate) fn shutdown(&self) {
        Self::clear_task(&self.task);
    }

    fn set_error(error_slot: &Mutex<Option<crate::Error>>, error: crate::Error) {
        *error_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
    }

    fn replace_task(
        task_slot: &Mutex<Option<tokio::task::AbortHandle>>,
        task: tokio::task::AbortHandle,
    ) {
        if let Some(old_task) = task_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(task)
        {
            old_task.abort();
        }
    }

    fn clear_task(task_slot: &Mutex<Option<tokio::task::AbortHandle>>) {
        if let Some(task) = task_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }

    async fn handle_recv_error(
        error_slot: &Mutex<Option<crate::Error>>,
        notify: &Notify,
        db: &PgPool,
        error: sqlx::Error,
    ) -> bool {
        if util::is_connection_error(&error) {
            warn!(
                "Listening for task table changes stopped because the database connection was interrupted:\n{}",
                source_chain::to_string(&error)
            );
            sleep(LOST_CONNECTION_SLEEP).await;
            if let Err(error) = util::wait_for_reconnection(db, LOST_CONNECTION_SLEEP).await {
                warn!(
                    "Couldn't wait for the database to recover after listener failure:\n{}",
                    source_chain::to_string(&error)
                );
                Self::set_error(error_slot, error);
                notify.notify_one();
                true
            } else {
                warn!("Listening for task table changes resumed");
                false
            }
        } else if util::is_pool_timeout(&error) {
            warn!(
                "Listening for task table changes is waiting for a free database connection from the pool:\n{}",
                source_chain::to_string(&error)
            );
            sleep(LOST_CONNECTION_SLEEP).await;
            false
        } else {
            warn!(
                "Listening for task table changes failed:\n{}",
                source_chain::to_string(&error)
            );
            Self::set_error(error_slot, crate::Error::ListenerReceive(error));
            notify.notify_one();
            true
        }
    }

    #[cfg(test)]
    pub(crate) fn set_error_for_tests(&self, error: crate::Error) {
        Self::set_error(&self.error, error);
    }

    #[cfg(test)]
    pub(crate) fn set_error_and_notify_for_tests(&self, error: crate::Error) {
        Self::set_error(&self.error, error);
        self.notify.notify_one();
    }

    #[cfg(test)]
    pub(crate) fn stop_worker_for_tests(&self) {
        self.stop_worker.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }

    #[cfg(test)]
    pub(crate) fn set_task_for_tests(&self, task: tokio::task::AbortHandle) {
        Self::replace_task(&self.task, task);
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        Self::clear_task(&self.task);
    }
}

impl<'a> Subscription<'a> {
    pub async fn wait_for(self, period: Duration) {
        trace!("⌛Waiting for a listener wakeup for {period:?}");
        match timeout(period, self.0).await {
            Ok(_) => trace!("⚡Received a listener wakeup"),
            Err(_) => trace!("⏰The waiting timeout has expired"),
        }
    }

    pub async fn wait_forever(self) {
        trace!("⌛Waiting for a listener wakeup");
        self.0.await;
        trace!("⚡Received a listener wakeup");
    }
}

#[cfg(test)]
mod tests {
    use super::Listener;
    use crate::Error;
    use chrono::{DateTime, Utc};
    use sqlx::{postgres::PgPoolOptions, types::Uuid, PgPool, Row};
    use std::{future::pending, sync::Mutex, time::Duration};
    use tokio::{
        sync::Notify,
        time::{sleep, timeout},
    };

    #[tokio::test]
    async fn terminal_errors_wake_future_subscribers() {
        let listener = Listener::new();
        listener.set_error_and_notify_for_tests(Error::ListenerReceive(sqlx::Error::Protocol(
            "listener failed".into(),
        )));

        timeout(
            Duration::from_millis(50),
            listener.subscribe().wait_forever(),
        )
        .await
        .unwrap();

        assert!(matches!(
            listener.take_error(),
            Some(Error::ListenerReceive(sqlx::Error::Protocol(_)))
        ));
    }

    #[tokio::test]
    async fn pool_timeouts_do_not_become_terminal_listener_errors() {
        let error_slot = Mutex::new(None);
        let notify = Notify::new();
        let db = PgPoolOptions::new()
            .connect_lazy("postgres:///pg_task")
            .unwrap();

        assert!(
            !Listener::handle_recv_error(&error_slot, &notify, &db, sqlx::Error::PoolTimedOut)
                .await
        );
        assert!(error_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none());
    }

    #[tokio::test]
    async fn dropping_listener_aborts_background_task() {
        let listener = Listener::new();
        let task = tokio::spawn(pending::<()>());
        listener.set_task_for_tests(task.abort_handle());

        drop(listener);

        let error = timeout(Duration::from_millis(50), task)
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.is_cancelled());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn listen_wakes_subscribers_for_task_inserts(pool: PgPool) {
        let listener = Listener::new();
        listener.listen(pool.clone()).await.unwrap();

        let subscription = listener.subscribe();
        sqlx::query("INSERT INTO pg_task (step, wakeup_at) VALUES ($1, $2)")
            .bind("{}")
            .bind(Utc::now())
            .execute(&pool)
            .await
            .unwrap();

        timeout(Duration::from_secs(1), subscription.wait_forever())
            .await
            .unwrap();

        assert!(!listener.time_to_stop_worker());
        assert!(listener.take_error().is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn stop_worker_notifications_wake_future_subscribers(pool: PgPool) {
        let listener = Listener::new();
        listener.listen(pool.clone()).await.unwrap();

        sqlx::query("SELECT pg_notify('pg_task_changed', 'stop_worker')")
            .execute(&pool)
            .await
            .unwrap();

        timeout(Duration::from_secs(1), async {
            loop {
                if listener.time_to_stop_worker() {
                    return;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        timeout(
            Duration::from_millis(50),
            listener.subscribe().wait_forever(),
        )
        .await
        .unwrap();

        assert!(listener.take_error().is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn updating_tasks_refreshes_updated_at(pool: PgPool) {
        let row = sqlx::query(
            "
            INSERT INTO pg_task (step, wakeup_at)
            VALUES ($1, $2)
            RETURNING id, updated_at
            ",
        )
        .bind("{}")
        .bind(Utc::now())
        .fetch_one(&pool)
        .await
        .unwrap();
        let id: Uuid = row.get("id");
        let initial_updated_at: DateTime<Utc> = row.get("updated_at");

        sleep(Duration::from_millis(20)).await;

        let next_updated_at: DateTime<Utc> = sqlx::query(
            "
            UPDATE pg_task
            SET error = $2
            WHERE id = $1
            RETURNING updated_at
            ",
        )
        .bind(id)
        .bind("boom")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("updated_at");

        assert!(next_updated_at > initial_updated_at);
    }
}
