use crate::{util, LOST_CONNECTION_SLEEP};
use parking_lot::Mutex;
use sqlx::{postgres::PgListener, PgPool};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
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
    #[cfg(test)]
    fail_listen: Arc<AtomicBool>,
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
            #[cfg(test)]
            fail_listen: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Connects to the db and starts to listen to tasks table changes
    pub async fn listen(&self, db: PgPool) -> crate::Result<()> {
        let mut listener = PgListener::connect_with(&db)
            .await
            .map_err(crate::Error::ListenerConnect)?;
        #[cfg(test)]
        Self::listen_on_channel(&mut listener, &self.fail_listen)
            .await
            .map_err(crate::Error::ListenerListen)?;
        #[cfg(not(test))]
        Self::listen_on_channel(&mut listener)
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
        self.error.lock().take()
    }

    pub(crate) fn shutdown(&self) {
        Self::clear_task(&self.task);
    }

    fn set_error(error_slot: &Mutex<Option<crate::Error>>, error: crate::Error) {
        *error_slot.lock() = Some(error);
    }

    fn replace_task(
        task_slot: &Mutex<Option<tokio::task::AbortHandle>>,
        task: tokio::task::AbortHandle,
    ) {
        let old_task = task_slot.lock().replace(task);
        if let Some(old_task) = old_task {
            old_task.abort();
        }
    }

    fn clear_task(task_slot: &Mutex<Option<tokio::task::AbortHandle>>) {
        let task = task_slot.lock().take();
        if let Some(task) = task {
            task.abort();
        }
    }

    #[cfg(test)]
    async fn listen_on_channel(
        listener: &mut PgListener,
        fail_listen: &AtomicBool,
    ) -> Result<(), sqlx::Error> {
        if fail_listen.swap(false, Ordering::SeqCst) {
            return Err(sqlx::Error::Protocol("listener listen failure".into()));
        }
        listener.listen(NOTIFICATION_CHANNEL).await
    }

    #[cfg(not(test))]
    async fn listen_on_channel(listener: &mut PgListener) -> Result<(), sqlx::Error> {
        listener.listen(NOTIFICATION_CHANNEL).await
    }

    async fn handle_recv_error(
        error_slot: &Mutex<Option<crate::Error>>,
        notify: &Notify,
        db: &PgPool,
        error: sqlx::Error,
    ) -> bool {
        match util::db_interruption(&error) {
            util::DbInterruption::Connection => {
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
            }
            util::DbInterruption::PoolTimeout => {
                warn!(
                    "Listening for task table changes is waiting for a free database connection from the pool:\n{}",
                    source_chain::to_string(&error)
                );
                sleep(LOST_CONNECTION_SLEEP).await;
                false
            }
            util::DbInterruption::Permanent => {
                warn!(
                    "Listening for task table changes failed:\n{}",
                    source_chain::to_string(&error)
                );
                Self::set_error(error_slot, crate::Error::ListenerReceive(error));
                notify.notify_one();
                true
            }
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

    #[cfg(test)]
    pub(crate) fn fail_next_listen_for_tests(&self) {
        self.fail_listen.store(true, Ordering::SeqCst);
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
mod tests;
