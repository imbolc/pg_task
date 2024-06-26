/// Converts a chrono duration to std, it uses absolute value of the chrono
/// duration
pub fn chrono_duration_to_std(chrono_duration: chrono::Duration) -> std::time::Duration {
    let seconds = chrono_duration.num_seconds();
    let nanos = chrono_duration.num_nanoseconds().unwrap_or(0) % 1_000_000_000;
    std::time::Duration::new(seconds.unsigned_abs(), nanos.unsigned_abs() as u32)
}

/// Converts a std duration to chrono
pub fn std_duration_to_chrono(std_duration: std::time::Duration) -> chrono::Duration {
    chrono::Duration::from_std(std_duration).unwrap_or_else(|_| chrono::Duration::max_value())
}

/// Returns the ordinal string of a given integer
pub fn ordinal(n: i32) -> String {
    match n.abs() {
        11..=13 => format!("{}-th", n),
        _ => match n % 10 {
            1 => format!("{}-st", n),
            2 => format!("{}-nd", n),
            3 => format!("{}-rd", n),
            _ => format!("{}-th", n),
        },
    }
}
