#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(clippy::all, missing_docs, nonstandard_style, future_incompatible)]

mod error;
mod listener;
mod macros;
mod next_step;
mod task;
mod traits;
mod util;
mod worker;

pub use error::{Error, Result, StepError, StepResult};
pub use next_step::NextStep;
pub use traits::{Scheduler, Step};
pub use worker::Worker;

use chrono::{DateTime, Utc};
use sqlx::{types::Uuid, PgExecutor};
use std::time::Duration;

const LOST_CONNECTION_SLEEP: Duration = Duration::from_secs(1);

/// Enqueues the task to be run immediately
pub async fn enqueue<'e>(db: impl PgExecutor<'e>, task: &impl Scheduler) -> Result<Uuid> {
    task.enqueue(db).await
}

/// Schedules a task to be run after a specified delay
pub async fn delay<'e>(
    db: impl PgExecutor<'e>,
    task: &impl Scheduler,
    delay: Duration,
) -> Result<Uuid> {
    task.delay(db, delay).await
}

/// Schedules a task to run at a specified time in the future
pub async fn schedule<'e>(
    db: impl PgExecutor<'e>,
    task: &impl Scheduler,
    at: DateTime<Utc>,
) -> Result<Uuid> {
    task.schedule(db, at).await
}
