use crate::StepResult;
use std::time::Duration;

/// Represents next step of the task
pub enum NextStep<T> {
    /// The task is done
    None,
    /// Run the next step immediately
    Now(T),
    /// Delay the next step
    Delayed(T, Duration),
}

impl<T> NextStep<T> {
    /// The task is done
    pub fn none() -> StepResult<T> {
        Ok(Self::None)
    }

    /// Run the next step immediately
    pub fn now(step: impl Into<T>) -> StepResult<T> {
        Ok(Self::Now(step.into()))
    }

    /// Delay the next step
    pub fn delay(step: impl Into<T>, delay: Duration) -> StepResult<T> {
        Ok(Self::Delayed(step.into(), delay))
    }
}
