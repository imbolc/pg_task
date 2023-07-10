//! # pg-fsm
//!
//! Resumable FSM-based Postgres tasks
//!
//!
//! # TODO
//! - [x] retry
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
