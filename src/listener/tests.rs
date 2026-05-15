use super::Listener;
use crate::Error;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use sqlx::{postgres::PgPoolOptions, types::Uuid, PgPool};
use std::{future::pending, io, time::Duration};
use tokio::{
    sync::Notify,
    time::{sleep, timeout},
};

fn connection_error() -> sqlx::Error {
    sqlx::Error::Io(io::Error::new(
        io::ErrorKind::BrokenPipe,
        "listener connection dropped",
    ))
}

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

#[tokio::test]
async fn listen_returns_connect_errors_for_unavailable_databases() {
    let listener = Listener::new();
    let db = PgPoolOptions::new()
        .connect_lazy(&format!(
            "postgres:///pg_task_missing_{}",
            Utc::now().timestamp_micros()
        ))
        .unwrap();

    let err = listener.listen(db).await.unwrap_err();

    assert!(matches!(err, Error::ListenerConnect(_)));
}

#[sqlx::test(migrations = "./migrations")]
async fn listen_returns_listener_listen_errors_when_subscribing_fails(pool: PgPool) {
    let listener = Listener::new();
    listener.fail_next_listen_for_tests();

    let err = listener.listen(pool).await.unwrap_err();

    assert!(matches!(
        err,
        Error::ListenerListen(sqlx::Error::Protocol(_))
    ));
}

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
async fn take_error_clears_stored_errors() {
    let listener = Listener::new();
    listener.set_error_for_tests(Error::ListenerReceive(sqlx::Error::Protocol(
        "listener failed".into(),
    )));

    assert!(matches!(
        listener.take_error(),
        Some(Error::ListenerReceive(sqlx::Error::Protocol(_)))
    ));
    assert!(listener.take_error().is_none());
}

#[tokio::test]
async fn wait_for_returns_when_a_wakeup_arrives_before_the_timeout() {
    let listener = Listener::new();
    let subscription = listener.subscribe();

    listener.stop_worker_for_tests();

    timeout(
        Duration::from_millis(50),
        subscription.wait_for(Duration::from_secs(1)),
    )
    .await
    .unwrap();
    assert!(listener.time_to_stop_worker());
}

#[tokio::test]
async fn wait_for_returns_when_the_timeout_expires_without_a_wakeup() {
    let listener = Listener::new();

    timeout(
        Duration::from_millis(100),
        listener.subscribe().wait_for(Duration::from_millis(10)),
    )
    .await
    .unwrap();
    assert!(!listener.time_to_stop_worker());
    assert!(listener.take_error().is_none());
}

#[tokio::test]
async fn pool_timeouts_do_not_become_terminal_listener_errors() {
    init_tracing();
    let error_slot = Mutex::new(None);
    let notify = Notify::new();
    let db = PgPoolOptions::new()
        .connect_lazy("postgres:///pg_task")
        .unwrap();

    assert!(
        !Listener::handle_recv_error(&error_slot, &notify, &db, sqlx::Error::PoolTimedOut).await
    );
    assert!(error_slot.lock().is_none());
}

#[tokio::test]
async fn terminal_recv_errors_are_stored_and_notify_waiters() {
    init_tracing();
    let error_slot = Mutex::new(None);
    let notify = Notify::new();
    let subscription = notify.notified();
    let db = PgPoolOptions::new()
        .connect_lazy("postgres:///pg_task")
        .unwrap();

    assert!(
        Listener::handle_recv_error(
            &error_slot,
            &notify,
            &db,
            sqlx::Error::Protocol("listener failed".into()),
        )
        .await
    );
    timeout(Duration::from_millis(50), subscription)
        .await
        .unwrap();

    assert!(matches!(
        error_slot.lock().take(),
        Some(Error::ListenerReceive(sqlx::Error::Protocol(_)))
    ));
}

#[sqlx::test(migrations = "./migrations")]
async fn connection_errors_resume_listening_after_the_database_recovers(pool: PgPool) {
    init_tracing();
    let error_slot = Mutex::new(None);
    let notify = Notify::new();
    let subscription = notify.notified();

    assert!(!Listener::handle_recv_error(&error_slot, &notify, &pool, connection_error(),).await);
    assert!(timeout(Duration::from_millis(50), subscription)
        .await
        .is_err());
    assert!(error_slot.lock().is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn connection_errors_become_terminal_when_reconnection_fails(pool: PgPool) {
    init_tracing();
    let error_slot = Mutex::new(None);
    let notify = Notify::new();
    let subscription = notify.notified();
    sqlx::query!("ALTER TABLE pg_task RENAME COLUMN id TO task_id")
        .execute(&pool)
        .await
        .unwrap();

    assert!(Listener::handle_recv_error(&error_slot, &notify, &pool, connection_error(),).await);
    timeout(Duration::from_millis(50), subscription)
        .await
        .unwrap();
    assert!(matches!(
        error_slot.lock().take(),
        Some(Error::Db(sqlx::Error::Database(_), _))
    ));
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

#[tokio::test]
async fn replacing_listener_task_aborts_the_previous_background_task() {
    let listener = Listener::new();
    let first_task = tokio::spawn(pending::<()>());
    let second_task = tokio::spawn(pending::<()>());
    listener.set_task_for_tests(first_task.abort_handle());
    listener.set_task_for_tests(second_task.abort_handle());

    let first_error = timeout(Duration::from_millis(50), first_task)
        .await
        .unwrap()
        .unwrap_err();
    assert!(first_error.is_cancelled());

    drop(listener);

    let second_error = timeout(Duration::from_millis(50), second_task)
        .await
        .unwrap()
        .unwrap_err();
    assert!(second_error.is_cancelled());
}

#[tokio::test]
async fn shutdown_aborts_the_background_task() {
    let listener = Listener::new();
    let task = tokio::spawn(pending::<()>());
    listener.set_task_for_tests(task.abort_handle());

    listener.shutdown();

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
    sqlx::query!(
        "INSERT INTO pg_task (step, wakeup_at) VALUES ($1, $2)",
        "{}",
        Utc::now(),
    )
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
async fn listen_wakes_subscribers_for_non_stop_notifications(pool: PgPool) {
    let listener = Listener::new();
    listener.listen(pool.clone()).await.unwrap();

    let subscription = listener.subscribe();
    sqlx::query!("NOTIFY pg_task_changed, 'wake'")
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
async fn listen_wakes_subscribers_for_task_inserts_and_updates(pool: PgPool) {
    let listener = Listener::new();
    listener.listen(pool.clone()).await.unwrap();
    let insert_subscription = listener.subscribe();
    let id = sqlx::query!(
        "INSERT INTO pg_task (step, wakeup_at) VALUES ($1, $2) RETURNING id",
        "{}",
        Utc::now(),
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .id;
    timeout(Duration::from_secs(1), insert_subscription.wait_forever())
        .await
        .unwrap();

    let update_subscription = listener.subscribe();
    sqlx::query!("UPDATE pg_task SET error = $2 WHERE id = $1", id, "boom",)
        .execute(&pool)
        .await
        .unwrap();
    timeout(Duration::from_secs(1), update_subscription.wait_forever())
        .await
        .unwrap();

    assert!(!listener.time_to_stop_worker());
    assert!(listener.take_error().is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn stop_worker_notifications_wake_future_subscribers(pool: PgPool) {
    let listener = Listener::new();
    listener.listen(pool.clone()).await.unwrap();

    sqlx::query!("NOTIFY pg_task_changed, 'stop_worker'")
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
async fn closing_the_pool_surfaces_listener_errors_to_subscribers(pool: PgPool) {
    let listener = Listener::new();
    listener.listen(pool.clone()).await.unwrap();

    let subscription = listener.subscribe();
    let close_pool = tokio::spawn({
        let pool = pool.clone();
        async move {
            pool.close().await;
        }
    });

    timeout(Duration::from_secs(1), subscription.wait_forever())
        .await
        .unwrap();
    close_pool.await.unwrap();

    assert!(matches!(
        listener.take_error(),
        Some(Error::ListenerReceive(sqlx::Error::PoolClosed))
    ));
}

#[sqlx::test(migrations = "./migrations")]
async fn closing_the_pool_retains_terminal_wakeups_for_future_subscribers(pool: PgPool) {
    let listener = Listener::new();
    listener.listen(pool.clone()).await.unwrap();

    let close_pool = tokio::spawn({
        let pool = pool.clone();
        async move {
            pool.close().await;
        }
    });

    timeout(Duration::from_secs(1), async {
        loop {
            if listener.error.lock().is_some() {
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
    close_pool.await.unwrap();

    assert!(matches!(
        listener.take_error(),
        Some(Error::ListenerReceive(sqlx::Error::PoolClosed))
    ));
}

#[sqlx::test(migrations = "./migrations")]
async fn updating_tasks_refreshes_updated_at(pool: PgPool) {
    let row = sqlx::query!(
        "
            INSERT INTO pg_task (step, wakeup_at)
            VALUES ($1, $2)
            RETURNING id, updated_at
            ",
        "{}",
        Utc::now(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let id: Uuid = row.id;
    let initial_updated_at: DateTime<Utc> = row.updated_at;

    sleep(Duration::from_millis(20)).await;

    let next_updated_at: DateTime<Utc> = sqlx::query!(
        "
            UPDATE pg_task
            SET error = $2
            WHERE id = $1
            RETURNING updated_at
            ",
        id,
        "boom",
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .updated_at;

    assert!(next_updated_at > initial_updated_at);
}
