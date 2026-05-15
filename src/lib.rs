/*!
# pg_task

FSM-based Resumable Postgres tasks

- **FSM-based** - each task is a granular state machine
- **Resumable** - on error, after you fix the step logic or the external world,
  the task is able to pick up where it stopped
- **Postgres** - a single table handles task scheduling, state
  transitions, and error processing

## Table of Contents

- [Tutorial](#tutorial)
    - [Defining Tasks](#defining-tasks)
    - [Investigating Errors](#investigating-errors)
    - [Fixing the World](#fixing-the-world)
- [Scheduling Tasks](#scheduling-tasks)
- [Running Workers](#running-workers)
- [Stopping Workers](#stopping-workers)
- [Delaying Steps](#delaying-steps)
- [Retrying Steps](#retrying-steps)

## Tutorial

_The full runnable code is in [examples/tutorial.rs][tutorial-example]._

### Defining Tasks

We create a greeter task consisting of two steps:

```rust
# use async_trait::async_trait;
# use pg_task::{NextStep, Step, StepResult};
# use serde::{Deserialize, Serialize};
# use sqlx::PgPool;
#[derive(Debug, Deserialize, Serialize)]
pub struct ReadName {
    filename: String,
}
# #[derive(Debug, Deserialize, Serialize)]
# pub struct SayHello {
#     name: String,
# }
# pg_task::task!(Greeter { ReadName, SayHello });
# #[async_trait]
# impl Step<Greeter> for SayHello {
#     async fn step(self, _db: &PgPool) -> StepResult<Greeter> {
#         NextStep::none()
#     }
# }

#[async_trait]
impl Step<Greeter> for ReadName {
    const RETRY_LIMIT: i32 = 5;

    async fn step(self, _db: &PgPool) -> StepResult<Greeter> {
        let name = std::fs::read_to_string(&self.filename)?;
        NextStep::now(SayHello { name })
    }
}
```

The first step tries to read a name from a file:

- `filename` - the only state we need in this step
- `impl Step<Greeter> for ReadName` - our step is a part of a `Greeter` task
- `RETRY_LIMIT` - the step is fallible, let's retry it a few times
- `NextStep::now(SayHello { name })` - move our task to the `SayHello` step
  right now

```rust
# use async_trait::async_trait;
# use pg_task::{NextStep, Step, StepResult};
# use serde::{Deserialize, Serialize};
# use sqlx::PgPool;
# #[derive(Debug, Deserialize, Serialize)]
# pub struct ReadName {
#     filename: String,
# }
#[derive(Debug, Deserialize, Serialize)]
pub struct SayHello {
    name: String,
}
# pg_task::task!(Greeter { ReadName, SayHello });
# #[async_trait]
# impl Step<Greeter> for ReadName {
#     async fn step(self, _db: &PgPool) -> StepResult<Greeter> {
#         NextStep::none()
#     }
# }
#[async_trait]
impl Step<Greeter> for SayHello {
    async fn step(self, _db: &PgPool) -> StepResult<Greeter> {
        println!("Hello, {}", self.name);
        NextStep::none()
    }
}
```

The second step prints the greeting and finishes the task returning
`NextStep::none()`.

The [full code][tutorial-example] includes the remaining setup. Run it with:

```bash
cargo run --example tutorial
```

### Investigating Errors

The example logs 6 attempts: the first try plus `RETRY_LIMIT` retries. Inspect
the row to see what happened:

```bash
~$ psql pg_task -c 'table pg_task'
-[ RECORD 1 ]------------------------------------------------
id              | cddf7de1-1194-4bee-90c6-af73d9206ce2
step            | {"Greeter":{"ReadName":{"filename":"name.txt"}}}
wakeup_at       | 2024-06-30 09:32:27.703599+06
tried           | 6
locked_by       |
lock_expires_at |
error           | No such file or directory (os error 2)
created_at      | 2024-06-30 09:32:22.628563+06
updated_at      | 2024-06-30 09:32:27.703599+06
```

- a non-null `error` field indicates that the task has errored and contains the
  error message
- the `step` field provides you with the information about a particular step and
  its state when the error occurred

### Fixing the World

The task failed because the file is missing. Create it:

```bash
echo 'Fixed World' >name.txt
```

Clear `error` to rerun the task:

```bash
psql pg_task -c 'update pg_task set error = null'
```

The worker reruns the task and prints the greeting from the final step.

### Scheduling Tasks

Scheduling a task means inserting a row into the `pg_task` table. You can do it
from `psql` or from code in any language.

The crate also provides helpers for first-step serialization and scheduling:

- [`enqueue`] - to run the task immediately
- [`delay`] - to run it with a delay
- [`schedule`] - to schedule it to a particular time

### Running Workers

After [defining](#defining-tasks) the steps of each task, we need to wrap them
into enums representing whole tasks via [`task!`]:

```rust
# use async_trait::async_trait;
# use pg_task::{NextStep, Step};
# use sqlx::PgPool;
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct StepA;
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct StepB;
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct StepC;
pg_task::task!(Task1 { StepA, StepB });
pg_task::task!(Task2 { StepC });
# #[async_trait]
# impl Step<Task1> for StepA {
#     async fn step(self, _db: &PgPool) -> pg_task::StepResult<Task1> {
#         NextStep::none()
#     }
# }
# #[async_trait]
# impl Step<Task1> for StepB {
#     async fn step(self, _db: &PgPool) -> pg_task::StepResult<Task1> {
#         NextStep::none()
#     }
# }
# #[async_trait]
# impl Step<Task2> for StepC {
#     async fn step(self, _db: &PgPool) -> pg_task::StepResult<Task2> {
#         NextStep::none()
#     }
# }
```

One more enum is needed to combine all the possible tasks:

```rust
# use async_trait::async_trait;
# use pg_task::{NextStep, Step};
# use sqlx::PgPool;
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct StepA;
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct StepB;
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct StepC;
# pg_task::task!(Task1 { StepA, StepB });
# pg_task::task!(Task2 { StepC });
# #[async_trait]
# impl Step<Task1> for StepA {
#     async fn step(self, _db: &PgPool) -> pg_task::StepResult<Task1> {
#         NextStep::none()
#     }
# }
# #[async_trait]
# impl Step<Task1> for StepB {
#     async fn step(self, _db: &PgPool) -> pg_task::StepResult<Task1> {
#         NextStep::none()
#     }
# }
# #[async_trait]
# impl Step<Task2> for StepC {
#     async fn step(self, _db: &PgPool) -> pg_task::StepResult<Task2> {
#         NextStep::none()
#     }
# }
pg_task::scheduler!(Tasks { Task1, Task2 });
```

Now we can run the worker:

```rust
# async fn demo(db: sqlx::PgPool) -> pg_task::Result<()> {
# use async_trait::async_trait;
# use pg_task::{NextStep, Step};
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct StepA;
# pg_task::task!(Task1 { StepA });
# pg_task::scheduler!(Tasks { Task1 });
# #[async_trait]
# impl Step<Task1> for StepA {
#     async fn step(self, _db: &sqlx::PgPool) -> pg_task::StepResult<Task1> {
#         NextStep::none()
#     }
# }
pg_task::Worker::<Tasks>::new(db).run().await?;
# Ok(())
# }
```

Workers coordinate through Postgres, so you can run one or many of them, either
in separate processes or with [`tokio::spawn`].

### Stopping Workers

Gracefully stop workers by sending a notification through the database:

```sql
SELECT pg_notify('pg_task_changed', 'stop_worker');
```

Workers finish their current steps before exiting. To wait for them, check for
live leases:

```sql
SELECT EXISTS(
    SELECT 1
    FROM pg_task
    WHERE lock_expires_at > now()
);
```

### Delaying Steps

Sometimes you need to delay the next step. Using [`tokio::time::sleep`] before
returning the next step creates a couple of issues:

- if the process crashes while sleeping it won't be considered done and will
  rerun on restart
- you'd have to wait for the sleeping task to finish on
  [graceful shutdown](#stopping-workers)

Use [`NextStep::delay`] instead - it schedules the next step with the delay and
finishes the current one right away.

You can find a runnable example in the [examples/delay.rs][delay-example]

### Retrying Steps

Use [`Step::RETRY_LIMIT`] and [`Step::RETRY_DELAY`] when you need to retry a
task on errors:

```rust
# use async_trait::async_trait;
# use pg_task::{NextStep, Step, StepResult};
# use serde::{Deserialize, Serialize};
# use sqlx::PgPool;
# use std::time::Duration;
# #[derive(Debug, Deserialize, Serialize)]
# struct ProcessResult {
#     result: String,
# }
# #[derive(Debug, Deserialize, Serialize)]
# struct ApiRequest;
# pg_task::task!(MyTask { ApiRequest, ProcessResult });
# async fn api_request() -> Result<String, std::io::Error> {
#     Ok(String::from("ok"))
# }
# #[async_trait]
impl Step<MyTask> for ApiRequest {
    const RETRY_LIMIT: i32 = 5;
    const RETRY_DELAY: Duration = Duration::from_secs(5);

    async fn step(self, _db: &PgPool) -> StepResult<MyTask> {
        let result = api_request().await?;
        NextStep::now(ProcessResult { result })
    }
}
# #[async_trait]
# impl Step<MyTask> for ProcessResult {
#     async fn step(self, _db: &PgPool) -> StepResult<MyTask> {
#         NextStep::none()
#     }
# }
```

[delay-example]: https://github.com/imbolc/pg_task/blob/main/examples/delay.rs
[tutorial-example]:
    https://github.com/imbolc/pg_task/blob/main/examples/tutorial.rs
*/
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
