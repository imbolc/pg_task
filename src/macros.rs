/// Implements enum wrapper for a single task containing all it's steps
#[macro_export]
macro_rules! task {
    ($enum:ident { $($variant:ident),* }) => {
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
            async fn step(self, db: &sqlx::PgPool) -> $crate::StepResult<Option<$enum>> {
                Ok(match self {
                    $(Self::$variant(inner) => inner.step(db).await?.map(Into::into),)*
                })
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
    ($enum:ident { $($variant:ident),* }) => {
        $crate::task!($enum { $($variant),* });

        impl $enum {
            /// Enqueues the task to be run immediately
            pub async fn enqueue(&self, db: &$crate::PgPool) -> $crate::Result<$crate::Uuid> {
                self.schedule(db, $crate::chrono::Utc::now()).await
            }

            /// Schedules a task to be run after a specified delay
            async fn delay(
                &self,
                db: &$crate::PgPool,
                delay: std::time::Duration,
            ) -> $crate::Result<$crate::Uuid> {
                let delay = $crate::chrono::Duration::from_std(delay)
                    .unwrap_or_else(|_| $crate::chrono::Duration::max_value());
                self.schedule(db, $crate::chrono::Utc::now() + delay).await
            }

            /// Schedules a task to run at a specified time in the future
            async fn schedule(
                &self,
                db: &$crate::PgPool,
                at: $crate::chrono::DateTime<Utc>,
            ) -> $crate::Result<$crate::Uuid> {
                let step = serde_json::to_value(self).map_err($crate::Error::SerializeStep)?;
                sqlx::query!(
                    "INSERT INTO pg_task (step, wakeup_at) VALUES ($1, $2) RETURNING id",
                    step,
                    at
                )
                .map(|r| r.id)
                .fetch_one(db)
                .await
                .map_err($crate::sqlx_error!(format!("add task {:?}", self)))
                .map_err(Into::into)
            }
        }
    }
}
