use crate::NextStep;
use std::{error::Error as StdError, result::Result as StdResult};

/// The crate error
#[derive(Debug, displaydoc::Display, thiserror::Error)]
pub enum Error {
    /// can't add task
    AddTask(#[source] sqlx::Error),
    /// can't serialize step: {1}
    SerializeStep(#[source] serde_json::Error, String),
    /**
    can't deserialize step (the task was likely changed between the
    scheduling and running of the step): {1}
    */
    DeserializeStep(#[source] serde_json::Error, String),
    /// listener can't connect to the db
    ListenerConnect(#[source] sqlx::Error),
    /// can't start listening for table changes
    ListenerListen(#[source] sqlx::Error),
    /// listener can't receive table change notifications
    ListenerReceive(#[source] sqlx::Error),
    /// unreachable: worker semaphore is closed
    UnreachableWorkerSemaphoreClosed(#[source] tokio::sync::AcquireError),
    /// db error: {1}
    Db(#[source] sqlx::Error, String),
}

/// The crate result
pub type Result<T> = StdResult<T, Error>;

/// Error of a task step
pub type StepError = Box<dyn StdError + 'static + Send + Sync>;

/// Result returning from task steps
pub type StepResult<T> = StdResult<NextStep<T>, StepError>;

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn error_display_messages_are_stable() {
        assert_eq!(
            Error::Db(sqlx::Error::PoolTimedOut, "fetch task".into()).to_string(),
            "db error: fetch task",
        );
        assert_eq!(
            Error::ListenerReceive(sqlx::Error::PoolClosed).to_string(),
            "listener can't receive table change notifications",
        );
    }
}
