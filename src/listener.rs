use crate::{util, LOST_CONNECTION_SLEEP};
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

/// Waits for tasks table to change
pub struct Listener {
    notify: Arc<Notify>,
    stop_worker: Arc<AtomicBool>,
}

/// Subscription to the [`Listener`] notifications
pub struct Subscription<'a>(Notified<'a>);

impl Listener {
    /// Creates a waiter
    pub fn new() -> Self {
        let notify = Arc::new(Notify::new());
        let stop_worker = Arc::new(AtomicBool::new(false));
        Self {
            notify,
            stop_worker,
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
        tokio::spawn(async move {
            loop {
                match listener.recv().await {
                    Ok(msg) => {
                        if msg.payload() == STOP_WORKER_NOTIFICATION {
                            trace!("Got stop-worker notification");
                            stop_worker.store(true, Ordering::SeqCst);
                        }
                    }
                    Err(e) => {
                        warn!("Listening for the tasks table changes is interrupted (probably due to db connection loss):\n{}", source_chain::to_string(&e));
                        sleep(LOST_CONNECTION_SLEEP).await;
                        util::wait_for_reconnection(&db, LOST_CONNECTION_SLEEP).await;
                        warn!("Listening for the tasks table changes is probably restored");
                    }
                };
                notify.notify_waiters();
            }
        });
        Ok(())
    }

    /// Subscribes for notifications.
    ///
    /// Awaiting on the result ends on the first notification after the
    /// subscription, even if it happens between the subscription and awaiting.
    pub fn subscribe(&self) -> Subscription<'_> {
        Subscription(self.notify.notified())
    }

    /// Returns true if notification to stop worker is received
    pub fn time_to_stop_worker(&self) -> bool {
        self.stop_worker.load(Ordering::SeqCst)
    }
}

impl<'a> Subscription<'a> {
    pub async fn wait_for(self, period: Duration) {
        trace!("⌛Waiting for the tasks table to change for {period:?}");
        match timeout(period, self.0).await {
            Ok(_) => trace!("⚡The tasks table has changed"),
            Err(_) => trace!("⏰The waiting timeout has expired"),
        }
    }

    pub async fn wait_forever(self) {
        trace!("⌛Waiting for the tasks table to change");
        self.0.await;
        trace!("⚡The tasks table has changed");
    }
}
