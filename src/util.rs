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

/// Waits for the db reconnection
pub async fn wait_for_reconnection(db: &sqlx::PgPool, sleep: std::time::Duration) {
    while let Err(sqlx::Error::Io(_)) = sqlx::query!("SELECT id FROM pg_task LIMIT 1")
        .fetch_optional(db)
        .await
    {
        tracing::trace!("Waiting for db reconnection");
        tokio::time::sleep(sleep).await;
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
