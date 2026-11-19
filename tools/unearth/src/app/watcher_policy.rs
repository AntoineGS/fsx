use std::time::Duration;

pub(super) fn batch_delay() -> Result<Duration, String> {
    let value = match std::env::var("UNEARTH_WATCH_BATCH_MS") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(Duration::from_millis(125)),
        Err(error) => return Err(format!("UNEARTH_WATCH_BATCH_MS: {error}")),
    };
    value
        .parse::<u64>()
        .ok()
        .filter(|millis| (1..=30_000).contains(millis))
        .map(Duration::from_millis)
        .ok_or_else(|| "UNEARTH_WATCH_BATCH_MS must be an integer from 1 to 30000".to_string())
}

pub(super) fn periodic_reconcile_interval() -> Option<Duration> {
    match std::env::var("UNEARTH_WATCH_RECONCILE_SECS") {
        Ok(value) => periodic_reconcile_interval_from(Some(&value)),
        Err(_) => None,
    }
}

pub(super) fn periodic_reconcile_interval_from(value: Option<&str>) -> Option<Duration> {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(|seconds| (seconds > 0).then_some(Duration::from_secs(seconds)))
}
