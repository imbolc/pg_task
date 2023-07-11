//! # pg_task
//!
//! Resumable state machine based Postgres tasks
//!
//! # TODO
//! - [x] retry
//! - `pg_task::enqueue(Tasks::Count(Start { .. }.into()))` - bring back Scheduler trait
//! - [ ] log worker errors
//! - [ ] logging
//! - [ ] counurrency
//! - [ ] docs
//! - [ ] sqlx v7
//!
#![warn(clippy::all, missing_docs, nonstandard_style, future_incompatible)]

mod error;
mod macros;
mod traits;
mod worker;

pub use error::{Error, Result, StepError, StepResult};
pub use traits::Step;
pub use worker::Worker;

pub use chrono;
pub use sqlx::{types::Uuid, PgPool};
//TODO
pub use sqlx_error::sqlx_error;
