pub(crate) fn chrono_duration(std_duration: std::time::Duration) -> chrono::Duration {
    chrono::Duration::from_std(std_duration).unwrap_or_else(|_| chrono::Duration::max_value())
}
