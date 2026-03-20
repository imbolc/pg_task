/// Converts a chrono duration to std, it uses absolute value of the chrono
/// duration
pub fn chrono_duration_to_std(chrono_duration: chrono::Duration) -> std::time::Duration {
    let seconds = chrono_duration.num_seconds();
    let nanos = chrono_duration.num_nanoseconds().unwrap_or(0) % 1_000_000_000;
    std::time::Duration::new(seconds.unsigned_abs(), nanos.unsigned_abs() as u32)
}

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

/// Returns true if the SQLx error points to a lost or unavailable connection.
pub(crate) fn is_connection_error(error: &sqlx::Error) -> bool {
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
pub(crate) fn is_pool_timeout(error: &sqlx::Error) -> bool {
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
            Err(error) if is_connection_error(&error) || is_pool_timeout(&error) => {
                tracing::trace!("Waiting for a database connection to become available");
                tokio::time::sleep(sleep).await;
            }
            Err(error) => return Err(db_error!("wait for reconnection")(error)),
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
        is_connection_error, is_pool_timeout, is_retryable_database_error, wait_for_reconnection,
    };
    use sqlx::PgPool;
    use std::{io, time::Duration};

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
    fn protocol_violation_is_not_retryable() {
        assert!(!is_retryable_database_error(Some("08P01"), false));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn wait_for_reconnection_returns_permanent_errors(pool: PgPool) {
        sqlx::query("ALTER TABLE pg_task RENAME COLUMN id TO task_id")
            .execute(&pool)
            .await
            .unwrap();

        let err = wait_for_reconnection(&pool, Duration::from_millis(10))
            .await
            .unwrap_err();

        assert!(matches!(err, crate::Error::Db(sqlx::Error::Database(_), _)));
    }
}
