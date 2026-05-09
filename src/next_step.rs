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

#[cfg(test)]
mod tests {
    use super::NextStep;
    use std::time::Duration;

    #[test]
    fn none_returns_the_terminal_variant() {
        assert!(matches!(NextStep::<u8>::none().unwrap(), NextStep::None));
    }

    #[test]
    fn now_converts_input_into_the_target_step_type() {
        let next = NextStep::<u16>::now(7_u8).unwrap();

        assert!(matches!(next, NextStep::Now(7)));
    }

    #[test]
    fn delay_preserves_the_delay_and_converts_the_step() {
        let delay = Duration::from_millis(25);
        let next = NextStep::<u16>::delay(9_u8, delay).unwrap();

        match next {
            NextStep::Delayed(step, actual_delay) => {
                assert_eq!(step, 9);
                assert_eq!(actual_delay, delay);
            }
            _ => panic!("expected the delayed variant"),
        }
    }
}
