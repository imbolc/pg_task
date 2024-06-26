use sqlx::{postgres::PgListener, PgPool};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{futures::Notified, Notify},
    time::timeout,
};
use tracing::trace;

/// Waits for tasks table to change
pub struct Waiter(Arc<Notify>);
pub struct Subscription<'a>(Notified<'a>);

const PG_NOTIFICATION_CHANNEL: &str = "pg_task_changed";

impl Waiter {
    /// Creates a waiter
    pub fn new() -> Self {
        let notify = Arc::new(Notify::new());
        Self(notify)
    }

    /// Connects to the db and starts to listen to tasks table changes
    pub async fn listen(&self, db: PgPool) -> crate::Result<()> {
        let mut listener = PgListener::connect_with(&db)
            .await
            .map_err(crate::Error::WaiterListen)?;
        listener
            .listen(PG_NOTIFICATION_CHANNEL)
            .await
            .map_err(crate::Error::WaiterListen)?;

        let notify = self.0.clone();
        tokio::spawn(async move {
            loop {
                listener.recv().await.unwrap();
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
        Subscription(self.0.notified())
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
