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
    ($enum:ident { $($variant:ident),* }) => {
        $crate::task!($enum { $($variant),* });
        impl $crate::Scheduler for $enum {}
    }
}
