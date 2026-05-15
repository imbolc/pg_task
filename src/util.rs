/// Converts a std duration to chrono
pub fn std_duration_to_chrono(std_duration: std::time::Duration) -> chrono::Duration {
    chrono::Duration::from_std(std_duration).unwrap_or(chrono::Duration::MAX)
}

/// Returns the ordinal string of a given integer
pub fn ordinal(n: i32) -> String {
    match n.abs() {
        11..=13 => format!("{}th", n),
        _ => match n % 10 {
            1 => format!("{}st", n),
            2 => format!("{}nd", n),
            3 => format!("{}rd", n),
            _ => format!("{}th", n),
        },
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DbInterruption {
    Connection,
    PoolTimeout,
    Permanent,
}

/// Classifies whether a SQLx error should interrupt database work permanently
/// or be retried after waiting.
pub(crate) fn db_interruption(error: &sqlx::Error) -> DbInterruption {
    if is_connection_error(error) {
        DbInterruption::Connection
    } else if is_pool_timeout(error) {
        DbInterruption::PoolTimeout
    } else {
        DbInterruption::Permanent
    }
}

/// Returns true if the SQLx error points to a lost or unavailable connection.
fn is_connection_error(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Io(_) => true,
        sqlx::Error::Database(error) => is_retryable_database_error(
            error.code().as_deref(),
            error.is_transient_in_connect_phase(),
        ),
        _ => false,
    }
}

/// Returns true if the SQLx error indicates that the pool has no free
/// connections right now.
fn is_pool_timeout(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::PoolTimedOut)
}

fn is_retryable_database_error(code: Option<&str>, transient_in_connect_phase: bool) -> bool {
    transient_in_connect_phase
        || code.is_some_and(|code| {
            matches!(
                code,
                // connection_exception
                "08000" |
                // sqlclient_unable_to_establish_sqlconnection
                "08001" |
                // connection_does_not_exist
                "08003" |
                // sqlserver_rejected_establishment_of_sqlconnection
                "08004" |
                // connection_failure
                "08006" |
                // transaction_resolution_unknown
                "08007" |
                // admin_shutdown
                "57P01" |
                // crash_shutdown
                "57P02"
            )
        })
}

/// Waits until the database connection becomes available and returns early on
/// non-retryable errors.
pub async fn wait_for_reconnection(
    db: &sqlx::PgPool,
    sleep: std::time::Duration,
) -> crate::Result<()> {
    loop {
        match sqlx::query!("SELECT id FROM pg_task LIMIT 1")
            .fetch_optional(db)
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) => match db_interruption(&error) {
                DbInterruption::Connection | DbInterruption::PoolTimeout => {
                    tracing::trace!("Waiting for a database connection to become available");
                    tokio::time::sleep(sleep).await;
                }
                DbInterruption::Permanent => {
                    return Err(db_error!("wait for reconnection")(error));
                }
            },
        }
    }
}

/// A helper to construct db error
macro_rules! db_error {
    () => {
        |e| $crate::Error::Db(e, code_path::code_path!().into())
    };
    ($desc:expr) => {
        |e| $crate::Error::Db(e, format!("{} {}", code_path::code_path!(), $desc))
    };
}

pub(crate) use db_error;

#[cfg(test)]
mod tests {
    use super::{
        db_interruption, is_connection_error, is_pool_timeout, is_retryable_database_error,
        ordinal, std_duration_to_chrono, wait_for_reconnection, DbInterruption,
    };
    use chrono::Duration as ChronoDuration;
    use sqlx::{
        postgres::{PgConnectOptions, PgPoolOptions},
        PgPool,
    };
    use std::{io, time::Duration};

    // Short enough to exercise PoolTimedOut, but long enough for CI to open
    // the first TCP connection before the pool is intentionally exhausted.
    const POOL_TIMEOUT: Duration = Duration::from_millis(100);

    #[test]
    fn std_duration_to_chrono_saturates_on_overflow() {
        assert_eq!(std_duration_to_chrono(Duration::MAX), ChronoDuration::MAX);
    }

    #[test]
    fn ordinal_handles_teens_and_negative_numbers() {
        assert_eq!(ordinal(1), "1st");
        assert_eq!(ordinal(2), "2nd");
        assert_eq!(ordinal(12), "12th");
        assert_eq!(ordinal(23), "23rd");
        assert_eq!(ordinal(-4), "-4th");
    }

    #[test]
    fn transport_connection_errors_are_retryable() {
        assert!(is_connection_error(&sqlx::Error::Io(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "connection dropped",
        ))));
    }

    #[test]
    fn pool_timeouts_are_not_connection_errors() {
        assert!(!is_connection_error(&sqlx::Error::PoolTimedOut));
    }

    #[test]
    fn pool_timeouts_are_detected_separately() {
        assert!(is_pool_timeout(&sqlx::Error::PoolTimedOut));
    }

    #[test]
    fn db_interruption_classifies_retryable_errors() {
        assert_eq!(
            db_interruption(&sqlx::Error::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection dropped",
            ))),
            DbInterruption::Connection,
        );
        assert_eq!(
            db_interruption(&sqlx::Error::PoolTimedOut),
            DbInterruption::PoolTimeout,
        );
        assert_eq!(
            db_interruption(&sqlx::Error::Tls(
                io::Error::other("bad certificate").into(),
            )),
            DbInterruption::Permanent,
        );
    }

    #[test]
    fn permanent_non_database_errors_are_not_retryable() {
        assert!(!is_connection_error(&sqlx::Error::Tls(
            io::Error::other("bad certificate").into(),
        )));
    }

    #[test]
    fn database_connection_errors_are_retryable() {
        assert!(is_retryable_database_error(Some("08006"), false));
        assert!(is_retryable_database_error(Some("57P01"), false));
        assert!(is_retryable_database_error(Some("53300"), true));
    }

    #[test]
    fn documented_database_connection_error_codes_are_retryable() {
        for code in [
            "08000", "08001", "08003", "08004", "08006", "08007", "57P01", "57P02",
        ] {
            assert!(
                is_retryable_database_error(Some(code), false),
                "{code} should be retryable",
            );
        }
    }

    #[test]
    fn protocol_violation_is_not_retryable() {
        assert!(!is_retryable_database_error(Some("08P01"), false));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn wait_for_reconnection_returns_permanent_errors(pool: PgPool) {
        sqlx::query!("ALTER TABLE pg_task RENAME COLUMN id TO task_id")
            .execute(&pool)
            .await
            .unwrap();

        let err = wait_for_reconnection(&pool, Duration::from_millis(10))
            .await
            .unwrap_err();

        assert!(matches!(err, crate::Error::Db(sqlx::Error::Database(_), _)));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn wait_for_reconnection_returns_when_the_database_is_available(pool: PgPool) {
        wait_for_reconnection(&pool, Duration::from_millis(10))
            .await
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn wait_for_reconnection_retries_pool_timeouts_until_the_database_is_available(
        pool: PgPool,
    ) {
        let db_name: String = sqlx::query_scalar!(r#"SELECT current_database() AS "db_name!""#)
            .fetch_one(&pool)
            .await
            .unwrap();
        let retry_pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(POOL_TIMEOUT)
            .connect_with(current_database_options(&db_name))
            .await
            .unwrap();
        let held_connection = retry_pool.acquire().await.unwrap();
        let wait_pool = retry_pool.clone();
        let waiter = tokio::spawn(async move {
            wait_for_reconnection(&wait_pool, Duration::from_millis(10)).await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());

        drop(held_connection);

        waiter.await.unwrap().unwrap();
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
}
