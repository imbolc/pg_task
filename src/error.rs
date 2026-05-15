use crate::NextStep;
use std::{error::Error as StdError, result::Result as StdResult};

/// The crate error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Can't add a task.
    #[error("can't add task")]
    AddTask(#[source] sqlx::Error),
    /// Can't serialize the task step.
    #[error("can't serialize step: {1}")]
    SerializeStep(#[source] serde_json::Error, String),
    /// Can't deserialize the task step.
    #[error(
        "can't deserialize step (the task was likely changed between the scheduling and running of the step): {1}"
    )]
    DeserializeStep(#[source] serde_json::Error, String),
    /// Listener can't connect to the database.
    #[error("listener can't connect to the db")]
    ListenerConnect(#[source] sqlx::Error),
    /// Can't start listening for table changes.
    #[error("can't start listening for table changes")]
    ListenerListen(#[source] sqlx::Error),
    /// Listener can't receive table change notifications.
    #[error("listener can't receive table change notifications")]
    ListenerReceive(#[source] sqlx::Error),
    /// Worker semaphore is closed.
    #[error("unreachable: worker semaphore is closed")]
    UnreachableWorkerSemaphoreClosed(#[source] tokio::sync::AcquireError),
    /// Task lease expired before the worker could renew it.
    #[error("task lease expired before the worker could renew it")]
    TaskLeaseExpired,
    /// Database operation failed.
    #[error("db error: {1}")]
    Db(#[source] sqlx::Error, String),
}

/// The crate result
pub type Result<T> = StdResult<T, Error>;

/// Error of a task step
pub type StepError = Box<dyn StdError + 'static + Send + Sync>;

/// Result returning from task steps
pub type StepResult<T> = StdResult<NextStep<T>, StepError>;
