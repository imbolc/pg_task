use async_trait::async_trait;
use pg_task::{NextStep, Step, StepResult};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

mod util;

// Creates an enum `Greeter` containing our task steps
pg_task::task!(Greeter { ReadName, SayHello });

// Creates an enum `Tasks` representing all the possible tasks
pg_task::scheduler!(Tasks { Greeter });

#[derive(Debug, Deserialize, Serialize)]
pub struct ReadName {
    filename: String,
}
#[async_trait]
impl Step<Greeter> for ReadName {
    const RETRY_LIMIT: i32 = 5;

    async fn step(self, _db: &PgPool) -> StepResult<Greeter> {
        let name = std::fs::read_to_string(self.filename)?;
        NextStep::now(SayHello { name })
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SayHello {
    name: String,
}
#[async_trait]
impl Step<Greeter> for SayHello {
    async fn step(self, _db: &PgPool) -> StepResult<Greeter> {
        println!("Hello, {}", self.name);
        NextStep::none()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = util::init().await?;

    // Let's schedule the task
    pg_task::enqueue(
        &db,
        &Tasks::Greeter(
            ReadName {
                filename: "name.txt".into(),
            }
            .into(),
        ),
    )
    .await?;

    // And run a worker
    pg_task::Worker::<Tasks>::new(db).run().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    fn lazy_pool() -> PgPool {
        PgPoolOptions::new()
            .connect_lazy("postgres:///pg_task")
            .unwrap()
    }

    fn temp_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "pg-task-tutorial-example-{}-{}.txt",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ))
    }

    #[tokio::test]
    async fn read_name_reads_the_file_and_advances_to_the_greeting() {
        let path = temp_path();
        std::fs::write(&path, "Alice").unwrap();

        let next = ReadName {
            filename: path.display().to_string(),
        }
        .step(&lazy_pool())
        .await
        .unwrap();

        std::fs::remove_file(path).unwrap();

        match next {
            NextStep::Now(Greeter::SayHello(SayHello { name })) => {
                assert_eq!(name, "Alice");
            }
            _ => panic!("expected the greeting step"),
        }
    }

    #[tokio::test]
    async fn read_name_returns_io_errors_for_missing_files() {
        let result = ReadName {
            filename: temp_path().display().to_string(),
        }
        .step(&lazy_pool())
        .await;

        match result {
            Ok(_) => panic!("expected the missing file read to fail"),
            Err(err) => {
                let io = err.downcast::<std::io::Error>().unwrap();
                assert_eq!(io.kind(), std::io::ErrorKind::NotFound);
            }
        }
    }

    #[tokio::test]
    async fn say_hello_finishes_the_task() {
        assert!(matches!(
            SayHello {
                name: "Alice".into()
            }
            .step(&lazy_pool())
            .await
            .unwrap(),
            NextStep::None
        ));
    }
}
