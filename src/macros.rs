/// Implements an enum wrapper for a single task containing all its steps
#[macro_export]
macro_rules! task {
    ($enum:ident { $($variant:ident),* $(,)? }) => {
        #[derive(Debug, serde::Deserialize, serde::Serialize)]
        pub enum $enum {
            $($variant($variant),)*
        }

        $(
            impl From<$variant> for $enum {
                fn from(inner: $variant) -> Self {
                    Self::$variant(inner)
                }
            }
        )*

        #[async_trait::async_trait]
        impl $crate::Step<$enum> for $enum {
            async fn step(self, db: &sqlx::PgPool) -> $crate::StepResult<$enum> {
                match self {
                    $(Self::$variant(inner) => inner.step(db).await.map(|next|
                        match next {
                            $crate::NextStep::None => $crate::NextStep::None,
                            $crate::NextStep::Now(x) => $crate::NextStep::Now(x.into()),
                            $crate::NextStep::Delayed(x, d) => $crate::NextStep::Delayed(x.into(), d),
                        }
                    ),)*
                }
            }

            fn retry_limit(&self) -> i32 {
                match self {
                    $(Self::$variant(inner) => inner.retry_limit(),)*
                }
            }

            fn retry_delay(&self) -> std::time::Duration {
                match self {
                    $(Self::$variant(inner) => inner.retry_delay(),)*
                }
            }
        }
    }
}

/// The macro implements the outer enum wrapper containing all the tasks
#[macro_export]
macro_rules! scheduler {
    ($enum:ident { $($variant:ident),* $(,)? }) => {
        $crate::task!($enum { $($variant),* });
        impl $crate::Scheduler for $enum {}
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{postgres::PgPoolOptions, PgPool};
    use std::time::Duration;

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct First;

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub(super) struct Second;

    crate::task!(MacroTask { First, Second });
    crate::scheduler!(MacroScheduler { MacroTask });

    #[async_trait::async_trait]
    impl crate::Step<MacroTask> for First {
        const RETRY_LIMIT: i32 = 3;
        const RETRY_DELAY: Duration = Duration::from_millis(25);

        async fn step(self, _db: &PgPool) -> crate::StepResult<MacroTask> {
            crate::NextStep::delay(Second, Self::RETRY_DELAY)
        }
    }

    #[async_trait::async_trait]
    impl crate::Step<MacroTask> for Second {
        async fn step(self, _db: &PgPool) -> crate::StepResult<MacroTask> {
            crate::NextStep::none()
        }
    }

    fn assert_scheduler<T: crate::Scheduler>() {}

    #[test]
    fn task_macro_implements_from_for_each_variant() {
        let task: MacroTask = First.into();

        assert!(matches!(task, MacroTask::First(First)));
    }

    #[test]
    fn scheduler_macro_implements_the_scheduler_trait() {
        assert_scheduler::<MacroScheduler>();
    }

    #[tokio::test]
    async fn task_macro_forwards_step_and_retry_metadata() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres:///pg_task")
            .unwrap();
        let task = MacroTask::First(First);

        assert_eq!(crate::Step::<MacroTask>::retry_limit(&task), 3);
        assert_eq!(
            crate::Step::<MacroTask>::retry_delay(&task),
            Duration::from_millis(25),
        );

        match crate::Step::<MacroTask>::step(task, &pool).await.unwrap() {
            crate::NextStep::Delayed(MacroTask::Second(Second), delay) => {
                assert_eq!(delay, Duration::from_millis(25));
            }
            _ => panic!("expected the delayed second step"),
        }
    }
}
