use std::{error::Error as StdError, result::Result as StdResult};
use tracing::error;

/// The crate error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Db error during task saving
    #[error("add task")]
    AddTask(#[source] sqlx::Error),
    /// Task serialization issue
    #[error("serialize step")]
    SerializeStep(#[source] serde_json::Error),
}

/// The crate result
pub type Result<T> = StdResult<T, Error>;

/// Error of a task step
pub type StepError = Box<dyn StdError + 'static>;

/// Result returning from task steps
pub type StepResult<T> = StdResult<T, StepError>;
