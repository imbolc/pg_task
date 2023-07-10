use std::{error::Error as StdError, result::Result as StdResult};
use tracing::error;

/// The crate error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Sqlx error with some context
    #[error(transparent)]
    Sqlx(#[from] sqlx_error::SqlxError),
    /// Task serialization issue - shouldn't ever happen
    #[error("serialize step")]
    SerializeStep(#[source] serde_json::Error),
    /// Task deserialization issue - could happen if task changed while the table isn't empty
    #[error("deserialize step")]
    DeserializeStep(#[source] serde_json::Error),
}

/// The crate result
pub type Result<T> = StdResult<T, Error>;

/// Error of a task step
pub type StepError = Box<dyn StdError + 'static>;

/// Result returning from task steps
pub type StepResult<T> = StdResult<T, StepError>;
