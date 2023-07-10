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
pub mod prelude;
mod traits;
mod utils;
mod worker;

pub(crate) use error::Result;
pub use error::{Error, StepError, StepResult};
pub use traits::{Scheduler, Step};
pub use worker::Worker;
