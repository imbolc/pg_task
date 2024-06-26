use crate::NextStep;
use std::{error::Error as StdError, result::Result as StdResult};

/// The crate error
#[derive(Debug, displaydoc::Display, thiserror::Error)]
pub enum Error {
    /// can't add task
    AddTask(#[source] sqlx::Error),
    /// can't serialize step
    SerializeStep(#[source] serde_json::Error),
    /// can't unlock stale tasks
    UnlockStaleTasks(#[source] sqlx::Error),
    /// waiter can't connect to the db
    WaiterConnect(#[source] sqlx::Error),
    /// waiter can't start listening to tables changes
    WaiterListen(#[source] sqlx::Error),
}

/// The crate result
pub type Result<T> = StdResult<T, Error>;

/// Error of a task step
pub type StepError = Box<dyn StdError + 'static>;

/// Result returning from task steps
pub type StepResult<T> = StdResult<NextStep<T>, StepError>;
