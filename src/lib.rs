//! # pg_task
//!
//! Resumable state machine based Postgres tasks
#![warn(clippy::all, missing_docs, nonstandard_style, future_incompatible)]

mod error;
mod macros;
mod traits;
mod worker;

pub use error::{Error, Result, StepError, StepResult};
pub use traits::{Scheduler, Step};
pub use worker::Worker;

use chrono::{DateTime, Utc};
use sqlx::{types::Uuid, PgPool};
use std::time::Duration;

/// Enqueues the task to be run immediately
pub async fn enqueue(db: &PgPool, task: &impl Scheduler) -> Result<Uuid> {
    task.enqueue(db).await
}

/// Schedules a task to be run after a specified delay
pub async fn delay(db: &PgPool, task: &impl Scheduler, delay: Duration) -> Result<Uuid> {
    task.delay(db, delay).await
}

/// Schedules a task to run at a specified time in the future
pub async fn schedule(db: &PgPool, task: &impl Scheduler, at: DateTime<Utc>) -> Result<Uuid> {
    task.schedule(db, at).await
}
