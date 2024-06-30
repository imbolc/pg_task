//! # pg_task
//!
//! FSM-based Resumable Postgres tasks
//!
//! - **FSM-based** - each task is a granular state machine
//! - **Resumable** - on error, after you fix the step logic or the external
//!   world, the task is able to pick up where it stopped
//! - **Postgres** - a single table is enough to handle task scheduling, state
//!   transitions, and error processing
//!
//! ## Table of Contents
//!
//! - [Tutorial](#tutorial)
//!   - [Defining Tasks](#defining-tasks)
//!   - [Investigating Errors](#investigating-errors)
//!   - [Fixing the World](#fixing-the-world)
//! - [Scheduling Tasks](#scheduling-tasks)
//! - [Running Workers](#running-workers)
//! - [Stopping Workers](#stopping-workers)
//! - [Delaying Steps](#delaying-steps)
//! - [Retrying Steps](#retrying-steps)
//!
//! ## Tutorial
//!
//! _The full runnable code is in [examples/tutorial.rs][tutorial-example]._
//!
//! ### Defining Tasks
//!
//! We create a greeter task consisting of two steps:
//!
//! ```rust,ignore
//! #[derive(Debug, Deserialize, Serialize)]
//! pub struct ReadName {
//!     filename: String,
//! }
//!
//! #[async_trait]
//! impl Step<Greeter> for ReadName {
//!     const RETRY_LIMIT: i32 = 5;
//!
//!     async fn step(self, _db: &PgPool) -> StepResult<Greeter> {
//!         let name = std::fs::read_to_string(&self.filename)?;
//!         NextStep::now(SayHello { name })
//!     }
//! }
//! ```
//!
//! The first step tries to read a name from a file:
//!
//! - `filename` - the only state we need in this step
//! - `impl Step<Greeter> for ReadName` - our step is a part of a `Greeter` task
//! - `RETRY_LIMIT` - the step is fallible, let's retry it a few times
//! - `NextStep::now(SayHello { name })` - move our task to the `SayHello` step
//!   right now
//!
//! ```rust,ignore
//! #[derive(Debug, Deserialize, Serialize)]
//! pub struct SayHello {
//!     name: String,
//! }
//! #[async_trait]
//! impl Step<Greeter> for SayHello {
//!     async fn step(self, _db: &PgPool) -> StepResult<Greeter> {
//!         println!("Hello, {}", self.name);
//!         NextStep::none()
//!     }
//! }
//! ```
//!
//! The second step prints the greeting and finishes the task returning
//! `NextStep::none()`.
//!
//! That's essentially all, except for some boilerplate you can find in the
//! [full code][tutorial-example]. Let's run it:
//!
//! ```bash
//! cargo run --example hello
//! ```
//!
//! ### Investigating Errors
//!
//! You'll see log messages about the 6 (first try + `RETRY_LIMIT`) attempts and
//! the final error message. Let's look into the DB to find out what happened:
//!
//! ```bash
//! ~$ psql pg_task -c 'table pg_task'
//! -[ RECORD 1 ]------------------------------------------------
//! id         | cddf7de1-1194-4bee-90c6-af73d9206ce2
//! step       | {"Greeter":{"ReadName":{"filename":"name.txt"}}}
//! wakeup_at  | 2024-06-30 09:32:27.703599+06
//! tried      | 6
//! is_running | f
//! error      | No such file or directory (os error 2)
//! created_at | 2024-06-30 09:32:22.628563+06
//! updated_at | 2024-06-30 09:32:27.703599+06
//! ```
//!
//! - a non-null `error` field indicates that the task has errored and contains
//!   the error message
//! - the `step` field provides you with the information about a particular step
//!   and its state when the error occurred
//!
//! ### Fixing the World
//!
//! In this case, the error is due to the external world state. Let's fix it by
//! creating the file:
//!
//! ```bash
//! echo 'Fixed World' > name.txt
//! ```
//!
//! To rerun the task, we just need to clear its `error`:
//!
//! ```bash
//! psql pg_task -c 'update pg_task set error = null'
//! ```
//!
//! You'll see the log messages about rerunning the task and the greeting
//! message of the final step. That's all 🎉.
//!
//! ## Scheduling Tasks
//!
//! Essentially scheduling a task is done by inserting a corresponding row into
//! the `pg_task` table. You can do in by hands from `psql` or code in any
//! language.
//!
//! There's also a few helpers to take care of the first step serialization and
//! time scheduling:
//! - [`enqueue`] - to run the task immediately
//! - [`delay`] - to run it with a delay
//! - [`schedule`] - to schedule it to a particular time
//!
//! ## Running Workers
//!
//! After [defining](#defining-tasks) the steps of each task, we need to
//! wrap them into enums representing whole tasks via [`task!`]:
//!
//! ```rust,ignore
//! pg_task::task!(Task1 { StepA, StepB });
//! pg_task::task!(Task2 { StepC });
//! ```
//!
//! One more enum is needed to combine all the possible tasks:
//!
//! ```rust,ignore
//! # use pg_task::NextStep;
//! pg_task::scheduler!(Tasks { Task1, Task2 });
//! ```
//!
//! Now we can run the worker:
//!
//! ```rust,ignore
//! pg_task::Worker::<Tasks>::new(db).run().await?;
//! ```
//!
//! All the communication is synchronized by the DB, so it doesn't matter how or
//! how many workers you run. It could be a separate process as well as
//! in-process `tokio::spawn`.
//!
//! ## Stopping Workers
//!
//! You can gracefully stop task runners by sending a notification using the
//! DB:
//!
//! ```sql
//! SELECT pg_notify('pg_task_changed', 'stop_worker');
//! ```
//!
//! The workers would wait until the current step of all the tasks is finished
//! and then exit. You can wait for this by checking for the existence of
//! running tasks:
//!
//! ```sql
//! SELECT EXISTS(SELECT 1 FROM pg_task WHERE is_running = true);
//! ```
//!
//! ## Delaying Steps
//!
//! Sometimes you need to delay the next step. Using [`tokio::time::sleep`]
//! before returning the next step creates a couple of issues:
//!
//! - if the process is crashed while sleeping it wont be considered done and
//!   will rerun on restart
//! - you'd have to wait for the sleeping task to finish on [graceful
//!   shutdown](#stopping-workers)
//!
//! Use [`NextStep::delay`] instead - it schedules the next step with the delay
//! and finishes the current one right away.
//!
//! ## Retrying Steps
//!
//! Use [`Step::RETRY_LIMIT`] and [`Step::RETRY_DELAY`] when you need to retry a
//! task on errors:
//!
//! ```rust,ignore
//! impl Step<MyTask> for ApiRequest {
//!     const RETRY_LIMIT: i32 = 5;
//!     const RETRY_DELAY: Duration = Duration::from_secs(5);
//!
//!     async fn step(self, _db: &PgPool) -> StepResult<MyTask> {
//!         let result = api_request().await?;
//!         NextStep::now(ProcessResult { result })
//!     }
//! }
//! ```
//! [tutorial-example]: https://github.com/imbolc/pg_task/blob/main/examples/tutorial.rs

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
use sqlx::{types::Uuid, PgPool};
use std::time::Duration;

const LOST_CONNECTION_SLEEP: Duration = Duration::from_secs(1);

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
